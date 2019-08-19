package com.tiantao.spark.batch.entity

import java.util

import com.tiantao.spark.batch.handler.BaseHandler
import scala.beans.BeanProperty

/**
  * 记录执行的功能
  * Created by tiantao on 2019/4/13.
  */
class FunctionDto(
                   @BeanProperty var clazzName: String = null, // 该功能对应的类名
                   @BeanProperty var desc: String = null, // 该功能对应的描述
                   @BeanProperty var params: Array[String] = null //该功能需要的参数
                 ) {

  /**
    * 初始化信息
    *
    * @param data
    */
  def init(data: util.HashMap[String, Object]): FunctionDto = {
    this.clazzName = data.get("clazzName").toString
    this.desc = data.get("desc").toString
    this.params = data.get("params").toString.split(",")
    this
  }

  /**
    * 获取handler
    * @return
    */
  def getHandler(): BaseHandler = {
    val clazz = Class.forName(clazzName)
    clazz.newInstance().asInstanceOf[BaseHandler]
  }

}
