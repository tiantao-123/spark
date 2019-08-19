package com.tiantao.spark.streaming.local.runs

import com.tiantao.spark.common.config.ConfigManager

/**
  * Created by tiantao on 2019/4/7.
  */
object Tests {
  def main(args: Array[String]): Unit = {
    ConfigManager.load("E:\\procedure-notes\\spark-project\\spark-streaming\\src\\main\\resources\\app.yml")

    while (true){
      println(ConfigManager.getString("spark.basic.switch"))
      Thread.sleep(1000L)
    }



//    val conf = new SparkConf()
//      .setAppName("spark-streaming_test")
//      .setMaster("local")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    val streamingContext = new StreamingContext(conf, Durations.seconds(3))
//
//    val kafkaProps: util.HashMap[String, Object] = ConfigManager.getMap("spark.kafka.params") //kafka的配置信息
//    kafkaProps.put("enable.auto.commit", (false: java.lang.Boolean))
//    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
//      streamingContext,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe(Array("test-tt1"), kafkaProps))
//
//    kafkaDStream.print()
//
//    streamingContext.start()
//    streamingContext.awaitTermination()

    //    // 测试ConfigManager类
    //    ConfigManager.load("E:\\procedure-notes\\spark-project\\spark-streaming\\src\\main\\resources\\app.yml")
    //    println(ConfigManager.getString("spark.switch"))
    //    println(ConfigManager.getInt("spark.streaming.params.spark.blockInterval"))
    //    println(ConfigManager.getMap("spark.kafka.params"))
    //    println(ConfigManager.getMap("spark.kafka.offset.params"))
    //    val bbb: util.List[util.HashMap[String, Object]] = ConfigManager.getList[util.HashMap[String, Object]]("spark.streaming.functions")
    //    println(ConfigManager.getList[util.HashMap[String, Object]]("spark.streaming.functions"))


  }
}
