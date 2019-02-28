package com.Utils

import org.apache.spark.rdd.RDD

/**
  * @BelongsProject: RechargeAnalyze
  * @BelongsPackage: com.Utils
  * @Author: Flourish Sang
  * @CreateTime: 2019-02-27 08:15
  * @Description: ${Description}
  */
object RequirementAnalyze {
  /**
    * 指标一
    */
  def requirement01(lines:RDD[(String,List[Double])]):Unit={
    lines.foreachPartition(part =>{
      val jedis = JedisConnectionPool.getConnection()
      //数字1用于统计充值订单量，充值金额，充值成功数，充值时长，充值成功数且金额不等于0
      part.foreach(t=>{
        //充值订单数
        jedis.hincrBy(t._1,"total",t._2(0).toLong)
        //充值总金额
        jedis.hincrByFloat(t._1,"money",t._2(1))
        //充值成功数
        jedis.hincrBy(t._1,"success",t._2(2).toLong)
        //充值总时长
        jedis.hincrBy(t._1,"time",t._2(3).toLong)

      })
      jedis.close()
    })
  }

  /**
    * 指标二
    */
  def requirement02(lines: RDD[((String,String),List[Double])]):Unit = {
    lines.foreachPartition(f=>{
      val jedis = JedisConnectionPool.getConnection()
      f.foreach(t=>{
        jedis.hincrBy(t._1._1,t._1._2,t._2.head.toLong - t._2(2).toLong)
      })
      jedis.close()
    })
  }

  /**
    * 指标三
    */
  def requirement03(lines:RDD[(String,List[Double])]):Unit={
    lines.sortBy(_._2.head,false)
      .map(t=>(t._1,(t._2(2)/t._2.head*100).formatted("%.1f")))
      .foreachPartition( t=>{
        //拿到连接
        val conn = JdbcMysql.getConn()
        t.foreach(t=>{
          val sql = "insert into CityTopN(city,cuccess)"+
          "values('"+t._1 + "',"+t._2.toDouble + ")"
          val state = conn.createStatement()
          state.executeUpdate(sql)
        })
        JdbcMysql.releaseCon(conn)
      })
  }

  /**
    * 指标四
    */
  def requirement04(lines:RDD[(String,List[Double])]):Unit = {
    lines.map(t => {
      CountMoney.cases(List(t._1,t._2(1),t._2(4)))
    }).map(t =>(t.map(_._1),t.map(t=>(t._2,t._3))))
      .reduceByKey((list1,list2)=>list1.zip(list2)
      .map(t => (t._1._1+t._2._1,t._1._2+t._2._2)))
      .foreachPartition(t =>{
        //拿到连接
        val conn = JdbcMysql.getConn()
        t.foreach(t => {
          val sql = "insert into RealTime(hour,count,money)"+
          "value('"+t._1+"','"+t._2.map(_._1.toInt)+"','"+t._2.map(_._2.toDouble)+"')"
          val state = conn.createStatement()
          state.executeUpdate(sql)
        })
        JdbcMysql.releaseCon(conn)
      })
  }


  /**
    * 指标五
    */
  def requirement05(lines: RDD[((String,String),List[Double])],myDate: String):Unit={
    lines.filter(_._1 == myDate).map(t=>{
      (t._1,t._2(0) - t._2(1),((t._2(0)-t._2(1))/t._2(0)*100).formatted("%.2f"))
    }).sortBy(_._2,false).foreachPartition(t=>{
      //拿到连接
      val conn = JdbcMysql.getConn()
      t.foreach(t=>{
        val sql = "insert into FailTopN(time,city,failnumber,failrate)"+
        "value('"+t._1._1 + "','"+t._1._2+"','"+t._2.toInt + ","+t._3.toDouble + ")"
        val state = conn.createStatement()
        state.executeUpdate(sql)
      })
      JdbcMysql.releaseCon(conn)
    })


  }
}
