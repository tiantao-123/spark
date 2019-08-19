package com.tiantao.spark.batch.handler

import com.tiantao.spark.batch.entity.FunctionDto
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}


/**
  * Created by tiantao on 2019/4/13.
  */
abstract class BaseHandler() {
  var sparkSession: SparkSession = _ //sparkSession
  var functionInfo: FunctionDto = _ // 功能信息
  var sparkContext: SparkContext = _
  var sqlContext: SQLContext = _

  /**
    * 初始化功能
    */
  final def init(sparkSession: SparkSession, functionInfo: FunctionDto): BaseHandler = {
    this.sparkSession = sparkSession
    this.functionInfo = functionInfo
    this.sparkContext = sparkSession.sparkContext
    this.sqlContext = sparkSession.sqlContext
    this.init()
    this
  }

  /**
    * 初始化资源(子类实现)
    */
  protected def init(): Unit = {}

  /**
    * 运行handler(子类实现)
    */
  def run(): Unit

  /**
    * 销毁资源(子类实现)
    */
  def destory(): Unit = {}

}
