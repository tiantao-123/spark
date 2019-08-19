package com.tiantao.spark.streaming.launcher

import java.net.URLDecoder
import java.util

import com.tiantao.spark.common.check.CheckUtils
import com.tiantao.spark.common.config.ConfigManager
import com.tiantao.spark.streaming.entity.FunctionDto
import com.tiantao.spark.streaming.handler.BaseHandler
import com.tiantao.spark.streaming.offset.RedisOffset
import com.tiantao.spark.streaming.utils.SparkUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.{BufferedSource, Source}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * spark streaming 启动类
  * Created by tiantao on 2019/4/7.
  */
object Launcher {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //1. 加载配置信息
    ConfigManager.load(args(0))
    val appName = ConfigManager.getString("spark.basic.appName", "spark-streaming")
    val groupId = ConfigManager.getString("spark.kafka.params.group.id")

    //2. 创建StreamingContext对象 和kafka的inputDStream对象
    val streamingContext: StreamingContext = SparkUtils.getStreamingContext();
    val kafkaInputStream: InputDStream[ConsumerRecord[String, String]] = SparkUtils.getKafkaDStream(streamingContext)

    //4 记录偏移量
    val kafkaDStream: DStream[ConsumerRecord[String, String]] = kafkaInputStream.transform(lineRDD => {
      if(!lineRDD.isEmpty()){
        // 获取当前kafka的偏移量
        val offsetRanges: Array[OffsetRange] = lineRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        // 保存偏移量到redis中
        RedisOffset.saveOffset(offsetRanges, groupId)
        // 确保结果都已经正确，且幂等地输出了，再提交
        kafkaInputStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
      lineRDD
    })

    //3. 转换数据格式,并缓存
    val kafkaDStream2 = kafkaDStream.map(record => (record.topic(), record.value().toString))
    kafkaDStream2.persist(StorageLevel.MEMORY_AND_DISK)

    //5. 执行功能列表
    //5.1 获取要执行的功能列表
    val functions: util.List[util.HashMap[String, Object]] = ConfigManager.getList[util.HashMap[String, Object]]("spark.streaming.functions") //获取要执行功能的列表
    //5.2 初始化功能信息
    val functionList: ArrayBuffer[Tuple2[FunctionDto, BaseHandler]] = ArrayBuffer()
    for (function <- functions) {
      val functionDto = new FunctionDto().init(function)
      functionList.add((functionDto, functionDto.getHandler().init(streamingContext, functionDto)))
    }
    //5.3 执行功能
    functionList.foreach(item => item._2.run(item._1.getTopic, kafkaDStream2))

    //6 启动Spark streaming程序
    streamingContext.start()

    //7. 关闭spark streaming程序
    val checkIntervalMillis = 1000
    var isStopped = false
    while (!isStopped) {
      isStopped = streamingContext.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped)
        logger.info(" spark streaming context is stopped. exiting application... ")
      else {
        logger.debug(s" spark streaming $appName running .... ")
      }
      if (!isStopped && stopFlag()) {
        //关闭刷新配置文件的线程
        ConfigManager.stopThread()
        //销毁资源
        functionList.foreach(item => item._2.destory())
        // 关闭
        streamingContext.stop(true, true)
        Thread.sleep(500L)
        println(s" spark streaming $appName is stopped!!!!!!!")
        System.exit(0)
      }
    }
  }

  /**
    * 是否停止作业标记
    */
  def stopFlag(): Boolean = {
    val switch = ConfigManager.getString("spark.basic.switch")
    if (switch.equals("Y")) {
      false
    } else {
      true
    }
  }

  /**
    * 获取功能列表
    *
    * @param functionFilePath
    * @return
    */
  def getFunctionList(functionFilePath: String): Array[FunctionDto] = {
    CheckUtils.returnTrueThrowException(functionFilePath == null, "请指定要执行的功能所在的文件路径！")
    val functionFilePath1 = URLDecoder.decode(functionFilePath, "utf-8")
    val file: BufferedSource = Source.fromFile(functionFilePath1)
    val arr = file.getLines()
      .filter(!_.startsWith("#"))
      .map(item => {
        val splits: Array[String] = item.split(" ")
        CheckUtils.returnFalseThrowException(splits.length >= 2, "每个功能必须有类全名和功能描述两个字段！")
        new FunctionDto(splits(0), splits(1), splits(2), splits)
      })
      .toArray
    file.close()
    arr
  }


}
