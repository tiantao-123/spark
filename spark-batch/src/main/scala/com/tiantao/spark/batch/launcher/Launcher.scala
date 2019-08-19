package com.tiantao.spark.batch.launcher

import java.util
import com.tiantao.spark.batch.entity.FunctionDto
import com.tiantao.spark.batch.handler.BaseHandler
import com.tiantao.spark.batch.utils.SparkUtils
import com.tiantao.spark.common.check.CheckUtils
import com.tiantao.spark.common.config.ConfigManager
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * Created by tiantao on 2019/4/7.
  */
object Launcher {
  def main(args: Array[String]): Unit = {
    //1. 加载配置文件
    CheckUtils.returnTrueThrowException(args.length == 0, "请指定要执行的功能所在的文件路径！")
    ConfigManager.load(args(0))

    //2. 创建sparkSession对象
    val session: SparkSession = SparkUtils.getSparkSession()

    //3. 获取要执行的功能列表
    val functions: util.List[util.HashMap[String, Object]] = ConfigManager.getList[util.HashMap[String, Object]]("spark.streaming.functions") //获取要执行功能的列表
    //初始化功能信息
    val functionList: ArrayBuffer[Tuple2[FunctionDto, BaseHandler]] = ArrayBuffer()
    for (function <- functions) {
      val functionDto = new FunctionDto().init(function)
      functionList.add((functionDto, functionDto.getHandler().init(session, functionDto)))
    }

    //4. 执行功能
    functionList.foreach(item => item._2.run())

    //5. 销毁所有资源
    functionList.foreach(item => item._2.destory())

    //6. 关闭spark程序
    session.stop()
  }

}
