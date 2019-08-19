package com.tiantao.spark.streaming.local.runs

import com.tiantao.spark.streaming.launcher.Launcher

object LocalLauncher {
  def main(args: Array[String]): Unit = {
    Launcher.main(Array("E:\\procedure-notes\\spark-project\\spark-streaming\\src\\main\\resources\\app.yml"))
  }
}
