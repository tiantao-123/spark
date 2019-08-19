package com.tiantao.spark.batch.handler.impl

import com.tiantao.spark.batch.handler.BaseHandler
import org.apache.spark.TaskContext

/**
  * Created by tiantao on 2019/4/13.
  */
class TestHandler extends BaseHandler {

  /**
    * 运行handler(子类实现)
    */
  override def run(): Unit = {
    val rdd = sparkContext.parallelize(functionInfo.params,5)
    rdd.foreach(item => {
      println(TaskContext.get.partitionId + ":       " + item)
    })
  }

}
