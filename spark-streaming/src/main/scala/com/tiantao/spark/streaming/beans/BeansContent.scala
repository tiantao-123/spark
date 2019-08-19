package com.tiantao.spark.streaming.beans

import com.tiantao.spark.common.check.CheckUtils
import com.tiantao.spark.common.config.ConfigManager
import com.tiantao.spark.common.helper.RedisHelper
import javax.ws.rs.ext.ParamConverter.Lazy


object BeansContent {

  /**
    * 创建Redis的Jedis连接池
    */
  @Lazy
  val redisHelper: RedisHelper = {
    //1. 获取配置信息
    val redisHost = ConfigManager.getString("spark.kafka.offset.params.redis.redisHost", null)
    val redisPort = ConfigManager.getInt("spark.kafka.offset.params.redis.redisPort", -1)
    val redisPwd = ConfigManager.getString("spark.kafka.offset.params.redis.redisPassword", null)
    //2. 校验配置信息
    CheckUtils.returnTrueThrowException(redisHost == null, "redis主机名不能为空！")
    CheckUtils.returnTrueThrowException(redisPort == -1, "redis端口号不能为空！")
    //3. 创建redisHelper
    new RedisHelper(redisHost, redisPort, redisPwd).createHelper()
  }

}
