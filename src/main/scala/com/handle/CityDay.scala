package com.handle

import com.Utils.RequirementAnalyze
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession



/**
  * @BelongsProject: RechargeAnalyze
  * @BelongsPackage: com.handle
  * @Author: Flourish Sang
  * @CreateTime: 2019-02-27 08:47
  * @Description: ${Description}
  */
object CityDay {
  def main(args: Array[String]): Unit = {
    if (args.length != 2){
      println("退出程序")
      sys.exit()
    }
    val Array(inputPath,myDate)=args
    //创建执行入口
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    //获取数据
    val file = sc.textFile(inputPath)

    //获取城市信息并广播
    val provinceInfo = sc.textFile("C://date/province.txt")
      .collect()
      .map(t=>{
        val arr = t.split(" ")
        (arr(0),arr(1))
      }).toMap
    val provInfoBroadcast = sc.broadcast(provinceInfo)
    //处理业务
    val lines = file.filter(t=>{
      val jsobj = JSON.parseObject(t)
      jsobj.getString("servinceName").equalsIgnoreCase("sendRechargeReq")
    })
      .map(t=>{
        val jsobj = JSON.parseObject(t)
        val result = jsobj.getString("bussinessRst")//充值结果
        val fee:Double = if (result.equals("00000"))
          jsobj.getDouble("chargefee") else 0.0 //充值金额
        val feeCount = if (!fee.equals(0.0)) 1 else 0 //获取充值成功数
        val starttime = jsobj.getString("requestId") //开始充值时间
        val pcode = jsobj.getString("provinceCode") //获取省份编号
        val province = provInfoBroadcast.value.getOrElse(pcode,"未知") //通过省份编号进行取值
        //充值成功数
        val isSucc = if (result.equals("0000"))1 else 0
        ((starttime.substring(0,8),province),List[Double](1,isSucc,feeCount))

      }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1 + t._2)
    })

    RequirementAnalyze.requirement05(lines,myDate)
  }

}
