package com.didi.etc.prop

import org.apache.spark.Logging
import com.didi.etc.prop.bean.PolygonUserConfBean

class PolygonStreamingProp(confName: String) extends BaseProp(confName) with Logging with Serializable {

  var userConfDir = ""
  var appName = ""
  var interval = 2000
  var directAPI = false
  var receverNum = 1
  var topic = ""
  var brokerList = ""
  var zookeeper = ""
  var group = ""
  /**
   * 解析基础属性
   */
  def getProp = {
    val PolygonStreaming = (xmlFile \ "PolygonStreaming")
    userConfDir = (PolygonStreaming \ "userConfDir").text
    appName = (PolygonStreaming \ "AppName").text
    interval = (PolygonStreaming \ "interval").text.toInt
    //-----kafka---------------------
    val kafka = (PolygonStreaming \ "kafka")
    directAPI = (kafka \ "directAPI").text.toBoolean
    receverNum = (kafka \ "receverNum").text.toInt
    topic = (kafka \ "topic").text
    brokerList = (kafka \ "brokerList").text
    zookeeper = (kafka \ "zookeeper").text
    group = (kafka \ "group").text
  //-----------------------------------
    val xmlConfBean = new PolygonUserConfBean
    xmlConfBean.setUserConfDir(userConfDir)
    xmlConfBean.setAppName(appName)
    xmlConfBean.setInterval(interval)
    // --------kafka----------------
    xmlConfBean.setDirectAPI(directAPI)
    xmlConfBean.setReceverNum(receverNum)
    xmlConfBean.setTopic(topic)
    xmlConfBean.setBrokerList(brokerList)
    xmlConfBean.setZookeeper(zookeeper)
    xmlConfBean.setGroup(group)

    printPorperties
    xmlConfBean
  }

  /**
   * 打印属app属性
   */
  override def printPorperties {
    super.printPorperties
    logInfo("============ polygonStreamConf.xml属性一览表 ==================")
    logInfo("用户配置文件路径:" + userConfDir)
    logInfo("streaming AppName:" + appName)
    logInfo("streaming 批次周期:" + interval)
    logInfo("是否使用kafka低阶api:" + directAPI)
    logInfo("低阶api情况下recever个数:" + receverNum)
    logInfo("kafka topic:" + topic)
    logInfo("kafka brokerList:" + brokerList)
    logInfo("zookeeper connect:" + zookeeper)
    logInfo("kafka group:" + zookeeper)
  }
}