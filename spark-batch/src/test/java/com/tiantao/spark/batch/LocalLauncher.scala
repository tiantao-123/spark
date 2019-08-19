package com.tiantao.spark.batch

import com.tiantao.spark.batch.launcher.Launcher

/**
  * Created by tiantao on 2019/4/13.
  */
object LocalLauncher {
  def main(args: Array[String]): Unit = {
    val args = Array("E:\\procedure-notes\\spark-project\\spark-batch\\src\\main\\resources\\app.yml")
    Launcher.main(args)
  }
}
