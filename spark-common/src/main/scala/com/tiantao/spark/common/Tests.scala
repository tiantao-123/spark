package com.tiantao.spark.common

import scala.collection.mutable.ArrayBuffer

/**
  * Created by tiantao on 2019/4/13.
  */
object Tests {
  def main(args: Array[String]): Unit = {
    val bbb = ArrayBuffer(1,2,3,4,5,6,7,8)
    bbb.remove(0,2)
    bbb.foreach(println(_))
  }
}
