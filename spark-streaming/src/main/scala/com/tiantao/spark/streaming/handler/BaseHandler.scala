package com.tiantao.spark.streaming.handler

import com.tiantao.spark.streaming.entity.FunctionDto
import javax.ws.rs.ext.ParamConverter.Lazy
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * Created by tiantao on 2019/4/13.
  */
abstract class BaseHandler() {
  var streamingContext: StreamingContext = _
  //streamingContext
  var functionInfo: FunctionDto = _ // 功能信息

  /**
    * 获取SparkSession
    *
    * @return
    */
  @Lazy
  def sparkSession: SparkSession = {
    SparkSession.builder().config(streamingContext.sparkContext.getConf).getOrCreate()
  }


  /**
    * 初始化功能
    */
  final def init(streamingContext: StreamingContext, functionInfo: FunctionDto): BaseHandler = {
    this.streamingContext = streamingContext
    this.functionInfo = functionInfo
    this.init();
    this
  }

  /**
    * 初始化资源(子类实现)
    */
  protected def init(): Unit = {}

  /**
    * 执行handler
    *
    * @param topic
    * @param kafkaDstream
    */
  final def run(topic: String, kafkaDstream: DStream[(String, String)]): Unit = {
    //1. 获取当前topic对应的数据
    val currDStream: DStream[(String, String)] = kafkaDstream.filter(_._1.equals(topic))
    //2. 运行handler
    this.run(currDStream)
  }

  /**
    * 运行handler(子类实现)
    */
  def run(dStream: DStream[(String, String)]): Unit

  /**
    * 销毁资源(子类实现)
    */
  def destory(): Unit = {}

}
