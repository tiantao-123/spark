package com.tiantao.spark.streaming.offset

import com.tiantao.spark.streaming.beans.BeansContent
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.{JavaConversions, mutable}

/**
  * 使用redis记录kafka的offset
  * redis 设计
  * kafka:offset:用户组Id:topic名称:Hash[分区,offset]
  */
object RedisOffset {

  val redisKeyBasic = "kafka:offset" //kafka 前缀

  /**
    * 获取offset信息
    *
    * @param topic
    * @param groupId
    * @return
    */
  def getOffset(topics: Array[String], groupId: String): (Map[TopicPartition, Long], Int) = {
    BeansContent.redisHelper.execute((jedis: Jedis) => {
      //1. 查询每个topic的所有分区的offset
      val fromOffsets = mutable.Map[TopicPartition, Long]()
      topics.foreach(topic => {
        val offset: scala.collection.mutable.Map[String, String] = jedis.hgetAll(s"${redisKeyBasic}:${groupId}:${topic}")
        if (!offset.isEmpty) {
          for ((partition, os) <- offset) {
            fromOffsets.put(new TopicPartition(topic, partition.toInt), os.toLong)
          }
        }
      })
      //2. 返回查询到的数据
      if (fromOffsets.isEmpty) {
        (fromOffsets.toMap, 0)
      } else {
        (fromOffsets.toMap, 1)
      }
    })
  }

  /**
    * 根据groupId保存offset
    *
    * @param jedis
    * @param rangs
    * @param groupId
    */
  def saveOffset(rangs: Array[OffsetRange], groupId: String): Unit = {
    BeansContent.redisHelper.execute((jedis: Jedis) => {
      // 2. 保存数据
      for (range <- rangs) {
        // 获取基本信息
        val topic: String = range.topic
        val partition: Int = range.partition
        val offset = range.untilOffset
        // 保存偏移量到redis中
        jedis.hset(s"${redisKeyBasic}:${groupId}:${topic}", partition.toString, offset.toString)
      }
    })
  }

}