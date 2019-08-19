package com.tiantao.spark.batch.utils

import java.util.Properties

import com.tiantao.spark.common.config.ConfigManager
import org.apache.spark.sql.SparkSession

/**
  * spark 工具类
  * Created by tiantao on 2019/4/13.
  */
object SparkUtils {
  /**
    * 创建SparkSession对象
    *
    * @param properties
    * @return
    */
  def getSparkSession(): SparkSession = {
    val appName = ConfigManager.getString("spark.basic.appName", "spark-batch")
    val sparkMaster: String = ConfigManager.getString("spark.basic.master", "local")

    if (sparkMaster.startsWith("local")) {
      SparkSession
        .builder
        .master(sparkMaster)
        .appName(appName)
        .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession
        .builder
        .appName(appName)
        .enableHiveSupport()
        .getOrCreate()
    }
  }
}
