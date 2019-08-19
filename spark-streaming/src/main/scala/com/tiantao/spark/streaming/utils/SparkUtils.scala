package com.tiantao.spark.streaming.utils

import java.util

import com.tiantao.spark.common.config.ConfigManager
import com.tiantao.spark.streaming.offset.RedisOffset
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.JavaConversions._

/**
  * Created by tiantao on 2019/4/14.
  */
object SparkUtils {

  /**
    * 创建SparkSession对象
    *
    * @param properties
    * @return
    */
  def getStreamingContext(): StreamingContext = {
    //1. 获取配置信息
    // spark的运行模式
    val master = ConfigManager.getString("spark.basic.master", "local[2]")
    // spark的应用名称
    val appName = ConfigManager.getString("spark.basic.appName", "spark-streaming")
    // batch的时间间隔
    val durations = ConfigManager.getInt("spark.basic.duration", 3)
    // spark 对象序列化方式
    val sparkSerializer = ConfigManager.getString("spark.basic.spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //checkPoint目录
    val checkPointPath = ConfigManager.getString("spark.basic.checkpoint.path")

    //2. 创建SparkConf对象
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.set("spark.serializer", sparkSerializer)
    if (master != null) {
      conf.setMaster(master)
    }
    // 设置配置文件里面的参数
    val sparkParams = ConfigManager.getMap("spark.basic.spark.conf")
    if (!sparkParams.isEmpty) {
      for ((k, v) <- sparkParams) {
        conf.set(k, v.toString)
      }
    }
    // 设置其他参数
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true") //设置sparkStreaming在接收到kill之后，会先把当前批次的程序执行完，再关闭
    conf.set("enable.auto.commit", "false") //设置不自动提交offset，让我们自己手动管理offset

    //3. 创建streamingContext对象
    val streamingContext = new StreamingContext(conf, Durations.seconds(durations))
    if (checkPointPath != null) {
      streamingContext.checkpoint(checkPointPath)
    }
    streamingContext
  }

  /**
    * 创建kafka与spark集成对象
    *
    * @param properties
    * @return
    */
  def getKafkaDStream(streamingContext: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    //1. 获取配置信息
    //kafka的配置信息
    val kafkaProps: util.HashMap[String, Object] = ConfigManager.getMap("spark.kafka.params")
    // 用户组Id
    val groupId: String = kafkaProps.get("group.id").toString
    //所有topic
    val topics: Array[String] = ConfigManager.getString("spark.kafka.topics").split(",")
    // 初始消费方式，latest（无提交的offset时，从新来的开始消费）
    val offsetReset = kafkaProps.get("auto.offset.reset")

    //2. 读取kafka的offset
    val (fromOffset: Map[TopicPartition, Long], flag: Int) = RedisOffset.getOffset(topics, groupId)

    //3. 创建InputDStream对象
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (flag == 1 && offsetReset.equals("latest")) {
      kafkaDStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics, kafkaProps, fromOffset))
    } else {
      kafkaDStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics, kafkaProps))
    }
    kafkaDStream
  }

}
