package com.Utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @BelongsProject: RechargeAnalyze
  * @BelongsPackage: com.Utils
  * @Author: Flourish Sang
  * @CreateTime: 2019-02-26 22:21
  * @Description: ${jedis；连接池}
  */
object JedisConnectionPool {
  //配置信息
  val config = new JedisPoolConfig
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //创建连接
  val pool = new JedisPool(config,"hadoop02",6370,10000)
  def getConnection():Jedis={
    pool.getResource//获取到连接
  }

}
