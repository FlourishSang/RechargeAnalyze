package com.Utils

import java.text.SimpleDateFormat

/**
  * @BelongsProject: RechargeAnalyze
  * @BelongsPackage: com.Utils
  * @Author: Flourish Sang
  * @CreateTime: 2019-02-26 22:26
  * @Description: ${Description}
  */
object TimeUtils {
  def costtime(starttime:String,receivertime: String):Long ={
    val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val startTime = sdf.parse(starttime.substring(0,17)).getTime
    val endTime = sdf.parse(receivertime).getTime
    endTime - startTime
  }
}
