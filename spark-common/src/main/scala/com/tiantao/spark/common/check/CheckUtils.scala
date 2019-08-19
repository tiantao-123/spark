package com.tiantao.spark.common.check

import com.tiantao.spark.common.exception.CheckException

/**
  * Created by tiantao on 2019/4/13.
  */
object CheckUtils {

  /**
    * 返回false，抛出校验异常
    * @param bool
    * @param errorMessage
    */
  def returnFalseThrowException(bool:Boolean,errorMessage:String){
    if(bool == false){
      throw new CheckException(errorMessage);
    }
  }

  /**
    * 返回true，抛出校验异常
    * @param bool
    * @param errorMessage
    */
  def returnTrueThrowException(bool:Boolean,errorMessage:String){
    if(bool == true){
      throw new CheckException(errorMessage);
    }
  }




}
