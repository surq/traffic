package com.didi.etc.its.common.util

object DataFormatUtil {

  val d1 = new java.text.DecimalFormat("#.0000")
  val d2 = new java.text.DecimalFormat("####.#")
  val df1 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val df2 = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
  val df3 = new java.text.SimpleDateFormat("yyyyMMdd HH:mm:ss")
  val df4 = new java.text.SimpleDateFormat("yy_MM_dd_HH_mm_ss")
  val df5 = new java.text.SimpleDateFormat("yyyyMMddHHmm")
  val date_YMD_1 = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val date_YMD_2 = new java.text.SimpleDateFormat("/yyyy/MM/dd")

  val date_Y = new java.text.SimpleDateFormat("yyyy")
  val date_M = new java.text.SimpleDateFormat("MM")
}