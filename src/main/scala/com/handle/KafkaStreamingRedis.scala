package com.handle

import com.Utils.{RequirementAnalyze, TimeUtils}
import com.alibaba.fastjson.JSON
import com.mysql.jdbc.TimeUtil
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.ivy.osgi.obr.xml.Requirement
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * @BelongsProject: RechargeAnalyze
  * @BelongsPackage: com.handle
  * @Author: Flourish Sang
  * @CreateTime: 2019-02-27 20:31
  * @Description: ${Description}
  */
object KafkaStreamingRedis {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    val ssc = new StreamingContext(session.sparkContext,Duration(5000))

    val load = ConfigFactory.load()
    //指定组名
    val groupId = load.getString("groupid")

    val topic = load.getString("topicid")

    //指定kafka的broker地址，（sparkstreaming的task直接连接到kafka的分区上，使用的是底层api进行消费
    val brokerList = load.getString("brokrt.list")

    //指定zk的列表，将offset维护到zk中
    val zkQuorum = load.getString("zookeeper.list")

    // 创建streaming时使用的是topic名字集合，sparkstraming可以同时消费多个topic）
    val topics : Set[String] = Set(topic)

    //创建一个zkgrouptopicdirs对象，其实是指定忘zk中写入数据的目录，用于保存偏移量
    val TopicDirs = new ZKGroupTopicDirs(groupId,topic)

    //获取zookeeper中的路径“/group01/offset/recharge
    val zkTopicPath = s"${TopicDirs.consumerOffsetDir}"

    //准备kafka参数
    val kafkas = Map(
      "metadata,broker.list" -> brokerList,
      "group.id" -> groupId,
      "auto.offset.reset"-> kafka.api.OffsetRequest.SmallestTimeString//指定读取数据方式
    )
    //创建zookeeper客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zkQuorum)

    val clientOffset = zkClient.countChildren(zkTopicPath)

    //创建kafkastreaming
    var kafkaStream:InputDStream[(String,String)] = null

    //如果zookeeper中保存offset，我们会利用这个offset作为kafkastreaming的起始位置
    var fromOffsets:Map[TopicAndPartition,Long] = Map()

    //如果保存过offset
    if (clientOffset>0){
      for (i<- 0 until clientOffset){
        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        val tp = TopicAndPartition(topic,i)

        //讲不同的partition对应的offset增加到fromsffset中
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      //key是kafka的key value就是kafka的数据
      //这个会将kafka的消息进行transform 最终kafka的数据都会变成（kafka的key，message）这样的Tuple
      val messageHandler = (mmd:MessageAndMetadata[String,String])=>
        (mmd.key(),mmd.message())
        //通过kafkautils创建直连的dstream
        // [String,String,StringDecoder, StringDecoder,(String,String)]
        // key    value  key解码方式     value的解码方式   接收数据的格式
        kafkaStream = KafkaUtils.createDirectStream
          [String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkas,fromOffsets,messageHandler)
      }else{
        //如果未保存，根据kafkas的配置使用最新的或者最旧的offset
        kafkaStream = KafkaUtils.createDirectStream
          [String, String, StringDecoder, StringDecoder](ssc, kafkas, topics)
      }
    //偏移量范围
    var offsetRanges = Array[OffsetRange]()
    //获取province数据并广播
    val provinceInfo = session.sparkContext
      .textFile("C://date/province.txt")
      .collect().map(t=>{
      val arr = t.split(" ")
      (arr(0),arr(1))
    }).toMap
    val provinceInfoBroadcast = session.sparkContext.broadcast(provinceInfo)

    kafkaStream.foreachRDD(rdd=>{
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val baseData = rdd.map(t=> JSON.parseObject(t._2))//获取实际的数据
        .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
        .map(jsobj=>{
          val rechargeRes = jsobj.getString("bussinessRst")//充值结果
          //判断是否充值成功
          val fee:Double = if (rechargeRes.equals("0000"))
            jsobj.getDouble("chargefee") else 0.0
          val feeCount = if (!fee.equals(0.0))1 else 0 //获取到充值成功数，金额不等于0
          val startTime = jsobj.getString("requestId")//开始充值时间
          val recivcetime = jsobj.getString("receiveNotifyTime")// 充值结束时间
          val pcode = jsobj.getString("provinceCode") //获取省份编号
          val province = provinceInfoBroadcast.value.get(pcode).toString //通过省份编号进行取值
          //充值成功数
          val isSucc = if (rechargeRes.equals("0000")) 1 else 0
          //充值时长
          val costtime = if (rechargeRes.equals("0000")) TimeUtils.costtime(startTime,recivcetime) else 0

          (startTime.substring(0,8),//年月日
          startTime.substring(0,10),//年月日时
            List[Double](1,fee,isSucc,costtime.toDouble,feeCount),//(数字1用于统计充值订单量，充值金额，充值成功数，充值时长，充值成功数且金额不等于0)
            province, // 省份
            startTime.substring(0,12), //年月日时分
            (startTime.substring(0,10),province)
          )
        }).cache()
      //指标一：要将两个list拉倒一起去，因为每次处理的结果都要合并
      val result1 = baseData.map(t=>(t._1,t._3)).reduceByKey((list1,list2)=>{
        //拉链操作：List(1,2,3) List(2,3,4) => List((1,2),(2,3),(3,4))
        list1.zip(list2).map(t=>t._1+t._2)
      })
      RequirementAnalyze.requirement01(result1)

      //指标二
      val result2 = baseData.map(t=>(t._6,t._3)).reduceByKey((list1,list2)=>{
        list1.zip(list2).map(t=>t._1+t._2)
      })
      RequirementAnalyze.requirement02(result2)

      //指标三
      val result3 = baseData.map(t=>(t._4,t._3)).reduceByKey((list1,list2)=>{
        list1.zip(list2).map(t=>t._1+t._2)
      })
      RequirementAnalyze.requirement03(result3)

      //指标四
      val result4 = baseData.map(t=>(t._5,t._3)).reduceByKey((list1,list2)=>{
        list1.zip(list2).map(t=> t._1+t._2)
      })
      RequirementAnalyze.requirement04(result4)
      //更新offset
      for (o <- offsetRanges){
        val zkpath = s"${TopicDirs.consumerOffsetDir}/${o.partition}"

        ZkUtils.updatePersistentPath(zkClient,zkpath,o.untilOffset.toString)
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }
}
