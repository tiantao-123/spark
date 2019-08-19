package com.tiantao.spark.common.config

import java.io.{FileInputStream, InputStreamReader}
import java.util

import com.tiantao.spark.common.exception.ConfigException
import org.slf4j.{Logger, LoggerFactory}
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions._

/**
  * 对配置文件的操作
  */
object ConfigManager {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var loadConfigInterval: Int = -1 //多久刷新一次配置文件,如果是-1，表示不刷新配置文件(单位毫秒)
  private var conf: util.HashMap[String, Object] = new util.HashMap[String, Object]()
  private val yml = new Yaml()
  private var loadConfigManager: LoadConfigManagerThread = null

  /**
    * 加载配置信息,并另外启动一个线程，实时刷新配置文件
    *
    * @param path
    * @return
    */
  def load(path: String) {
    //1. 第一次加载配置文件
    logger.debug("load app.yml file!")
    conf = yml.load(new InputStreamReader(new FileInputStream(path), "utf-8")).asInstanceOf[util.HashMap[String, Object]]
    //2. 获取配置文件中多久刷新一次配置文件的参数
    loadConfigInterval = this.getInt("conf.reflush.interval", -1)
    //3.开启线程，定时加载配置信息
    if (loadConfigInterval > 0) {
      this.loadConfigManager = new LoadConfigManagerThread()
      this.loadConfigManager.start(path)
    }
  }

  /**
    * 停止刷新配置文件的线程
    */
  def stopThread(): Unit = {
    if (this.loadConfigManager != null) {
      this.loadConfigManager.interrupt()        //设置该线程中断
    }
  }

  /**
    * 开启一个新的线程类，每隔10秒钟，加载一次配置文件
    */
  class LoadConfigManagerThread extends Thread {
    private var path: String = _

    def start(path: String): Unit = {
      this.path = path
      super.start()
    }

    override def run(): Unit = {
      while (!Thread.currentThread().isInterrupted()) {     //如果该线程中断
        Thread.sleep(loadConfigInterval)
        conf.synchronized {
          logger.debug("reflush app.yml file!")
          conf = yml.load(new InputStreamReader(new FileInputStream(path), "utf-8")).asInstanceOf[util.HashMap[String, Object]]
        }
      }
    }
  }

  /**
    * 读取配置信息
    *
    * @param key
    */
  def getString(key: String): String = {
    val result = this.get(key, conf)
    if (result == null) {
      null
    } else if (result.isInstanceOf[String]) {
      result.asInstanceOf[String]
    } else {
      result.toString
    }
  }

  /**
    * 读取配置信息，如果没有读取到，返回默认值
    *
    * @param key
    * @param default
    * @return
    */
  def getString(key: String, default: String): String = {
    val result = this.getString(key)
    if (result != null) {
      result
    } else {
      default
    }
  }

  /**
    * 读取配置信息
    *
    * @param key
    * @return
    */
  def getInt(key: String): Integer = {
    val result = this.get(key, conf)
    if (result == null) {
      null
    } else if (result.isInstanceOf[Int]) {
      result.asInstanceOf[Int]
    } else {
      throw new ConfigException("yml中的 " + key + " 配置不是Int类型！")
    }
  }

  /**
    * 读取配置信息，如果没有读取到，返回默认值
    *
    * @param key
    * @param default
    * @return
    */
  def getInt(key: String, default: Int): Int = {
    val result = this.getInt(key)
    if (result != null) {
      result
    } else {
      default
    }
  }

  /**
    * 读取配置信息
    *
    * @param key
    * @return
    */
  def getDouble(key: String): java.lang.Double = {
    val result = this.get(key, conf)
    if (result == null) {
      null
    } else if (result.isInstanceOf[Double]) {
      result.asInstanceOf[Double]
    } else {
      throw new ConfigException("yml中的 " + key + " 配置不是Double类型！")
    }
  }

  /**
    * 读取配置信息，如果没有读取到，返回默认值
    *
    * @param key
    * @param default
    * @return
    */
  def getDouble(key: String, default: Double): Double = {
    val result = this.getDouble(key)
    if (result != null) {
      result
    } else {
      default
    }
  }

  /**
    * 读取配置信息
    *
    * @param key
    * @return
    */
  def getMap(key: String): util.HashMap[String, Object] = {
    val result = this.get(key, conf)
    if (result == null) {
      null
    } else if (result.isInstanceOf[util.HashMap[String, Object]]) {
      result.asInstanceOf[util.HashMap[String, Object]]
    } else {
      throw new ConfigException("yml中的 " + key + " 配置不是util.HashMap[String, Object]类型！")
    }
  }

  /**
    * 读取配置信息，如果没有读取到，返回默认值
    *
    * @param key
    * @param default
    * @return
    */
  def getMap(key: String, default: util.HashMap[String, Object]): util.HashMap[String, Object] = {
    val result = this.getMap(key)
    if (result != null) {
      result
    } else {
      default
    }
  }

  /**
    * 读取配置信息
    *
    * @param key
    * @return
    */
  def getList[T](key: String): util.List[T] = {
    val result = this.get(key, conf)
    if (result == null) {
      null
    } else if (result.isInstanceOf[util.List[T]]) {
      result.asInstanceOf[util.List[T]]
    } else {
      throw new ConfigException("yml中的 " + key + " 配置不是util.List[T]类型！")
    }
  }

  /**
    * 读取配置信息，如果没有读取到，返回默认值
    *
    * @param key
    * @param default
    * @return
    */
  def getList[T](key: String, default: util.List[T]): util.List[T] = {
    val result: util.List[T] = this.getList(key)
    if (result != null) {
      result
    } else {
      default
    }
  }

  /**
    * 递归算法，获取配置信息
    *
    * @param key
    * @param map
    */
  private def get(key: String, map: util.HashMap[String, Object]): Object = {
    for ((k: String, v: Object) <- map) {
      if (key.equals(k)) {
        return v
      } else if (key.startsWith(k)) {
        val key2 = key.substring(k.length + 1, key.length)
        val result = this.get(key2, v.asInstanceOf[util.HashMap[String, Object]])
        if (result != null) {
          return result
        }
      }
    }
    null
  }

}
