package com.didi.etc.prop

import org.apache.spark.Logging
import com.didi.etc.prop.bean.JiNankakouLuKouCountBean

class JiNankakouLuKouCountProp(confName: String) extends BaseProp(confName) with Logging with Serializable {

  var appName = ""
  var cityId = ""
  var countInterval = 5
  var kakouItemList: Array[String] = Array[String]()
  var kakouDescribeList: Array[String] = Array[String]()
  var kakouIdList: Array[String] = Array[String]()
  var batchPattern = ""
  var kakouPath = ""
  var out_url = ""
  var out_username = ""
  var out_password = ""
  var out_table = ""

  /**
   * 解析基础属性
   */
  def getProp = {
    val appNode = (xmlFile \ "JiNankakouLuKouCount")
    appName = (appNode \ "AppName").text
    cityId = (appNode \ "cityId").text
    countInterval = (appNode \ "countInterval").text.toInt
    kakouItemList = ((appNode \ "kakouItem").text).split(",")

    val inPut = (appNode \ "inPut")

    kakouDescribeList = ((inPut \ "kakouDescribeList").text).split(",")

    kakouIdList = ((inPut \ "kakouIdList").text).split(";")
    batchPattern = (inPut \ "batchPattern").text
    val hdfs = (inPut \ "hdfs")

    kakouPath = (hdfs \ "kakouPath").text
    val outPut = (appNode \ "outPut")

    val mysql = (outPut \ "mysql")
    out_url = (mysql \ "out_url").text
    out_username = (mysql \ "out_username").text
    out_password = (mysql \ "out_password").text
    out_table = (mysql \ "out_table").text

    //-----------------------------------
    val xmlConfBean = new JiNankakouLuKouCountBean
    xmlConfBean.setAppName(appName)
    xmlConfBean.setCountInterval(countInterval)
    xmlConfBean.setCityId(cityId)
    xmlConfBean.setKakouItemList(kakouItemList)
    xmlConfBean.setKakouDescribeList(kakouDescribeList)
    xmlConfBean.setKakouIdList(kakouIdList)
    xmlConfBean.setBatchPattern(batchPattern)
    xmlConfBean.setKakouPath(kakouPath)
    xmlConfBean.setOut_url(out_url)
    xmlConfBean.setOut_username(out_username)
    xmlConfBean.setOut_password(out_password)
    xmlConfBean.setOut_table(out_table)

    printPorperties
    xmlConfBean
  }

  /**
   * 打印属app属性
   */
  override def printPorperties {
    super.printPorperties
    logInfo("============ JiNankakouLuKouCountConf.xml属性一览表 ==================")
    println("spark app 名称：" + appName)
    println("卡口城市ID：" + cityId)
    println("卡口流量统计周期：" + countInterval + "分钟")
    println("卡口字段列表一览：" + kakouItemList.mkString(","))
    println("路口列表一览：" + kakouDescribeList.mkString(","))
    println("卡口ID列表：" + kakouIdList.mkString(","))
    println("数据入理模式：" + batchPattern)
    println("卡口hdfs Path：" + kakouPath)
    println("写入mysql url:" + out_url)
    println("写入mysql username:" + out_username)
    println("写入mysql password:" + out_password)
    println("写入mysql username:" + out_table)
  }
}