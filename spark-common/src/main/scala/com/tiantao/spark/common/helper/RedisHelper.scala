package com.tiantao.spark.common.helper

import com.tiantao.spark.common.check.CheckUtils
import org.apache.log4j.Logger
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Redis帮助类
  */
class RedisHelper(
                   var host: String,
                   var port: Int,
                   var password: String
                 ) {
  val logger = Logger.getLogger(this.getClass)
  var pool: JedisPool = null //jedis连接池

  /**
    * 创建jedis连接池
    */
  def createHelper(): RedisHelper = {
    CheckUtils.returnTrueThrowException(host == null, "IP不能为空")
    CheckUtils.returnTrueThrowException(port == null, "端口号不能为空")

    val config = new JedisPoolConfig()
    this.pool = new JedisPool(config, this.host, this.port, 5000, this.password)
    this
  }

  /**
    * 操作redis
    *
    * @param function
    * @tparam T
    * @return
    */
  def execute[R](function: Function1[Jedis, R]): R = {
    var jedis: Jedis = null
    try {
      //1. 获取jedis对象
      jedis = this.getJedis()
      //2. 执行内容
      val t: R = function.apply(jedis)
      //3. 关闭jedis对象
      this.closeJedis(jedis)
      t
    } catch {
      case ex: Exception => {
        this.destoryJedis(jedis)
        throw new Exception(ex)
      }
    }
  }


  /**
    * 获取jedis对象
    *
    * @return
    */
  def getJedis(): Jedis = {
    pool.getResource
  }

  /**
    * 关闭jedis对象
    *
    * @param jedis
    */
  def closeJedis(jedis: Jedis): Unit = {
    jedis.close()
  }

  /**
    * 销毁某个jedis对象
    *
    * @param jedis
    */
  def destoryJedis(jedis: Jedis): Unit = {
    if (jedis != null) {
      jedis.quit()
      jedis.disconnect()
    }
  }

  /**
    * 销毁所有资源
    */
  def destoryPool(): Unit = {
    pool.close()
  }

}
