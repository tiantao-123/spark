package com.tiantao.spark.batch

import org.apache.spark.sql.SparkSession

/**
  * Created by tiantao on 2019/4/7.
  */
object Tests {
  def main(args: Array[String]): Unit = {
    //1. 创建SparkSession
    val sparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("spark-batch1")
      .getOrCreate()

    //2. 活动sparkContext
    val sc = sparkSession.sparkContext;

    //3. 整理数据
    val rdd = sc.parallelize(Array(12,23,34,5,5,76,5,3,4,23,2,37,232,4,35,45,665));

    //4. 处理数据
    rdd.map((_,1))
      .reduceByKey(_+_)
      .foreach(println(_));
  }
}
