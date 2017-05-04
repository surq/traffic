package com.didi.etc.prop

import org.apache.spark.Logging
import com.didi.etc.prop.bean.JunctionStreamingBean

class JunctionStreamingProp(confName: String) extends BaseProp(confName) with Logging with Serializable {

  var appName = ""
  var interval = 0
  var durationValue = 0
  var junctionList = ""
  var flowConfPath = ""
  var topic = ""
  var zookeeper = ""
  var group = ""
  var in_table = ""
  var url = ""
  var username = ""
  var password = ""
  var out_url = ""
  var out_username = ""
  var out_password = ""
  var out_table = ""
  /**
   * 解析基础属性
   */
  def getProp = {
    val JunctionStreaming = (xmlFile \ "JunctionStreaming")
    appName = (JunctionStreaming \ "AppName").text
    interval = (JunctionStreaming \ "interval").text.toInt
    val inPut = (JunctionStreaming \ "inPut")
    durationValue = (inPut \ "durationValue").text.toInt
    junctionList = (inPut \ "junctionList").text
    val hdfs = (inPut \ "hdfs")
    flowConfPath = (hdfs \ "flowConfPath").text
    val kafka = (inPut \ "kafka")
    topic = (kafka \ "topic").text
    zookeeper = (kafka \ "zookeeper").text
    group = (kafka \ "group").text

    val mysql = (inPut \ "mysql")
    in_table = (mysql \ "in_table").text
    url = (mysql \ "url").text
    username = (mysql \ "username").text
    password = (mysql \ "password").text

    val outPut = (JunctionStreaming \ "outPut")
    val outmysql = (outPut \ "mysql")
    out_url = (outmysql \ "out_url").text
    out_username = (outmysql \ "out_username").text
    out_password = (outmysql \ "out_password").text
    out_table = (outmysql \ "out_table").text

    //-----------------------------------
    val Jsbean = new JunctionStreamingBean
    Jsbean.setAppName(appName)
    Jsbean.setInterval(interval)
    Jsbean.setDurationValue(durationValue)
    Jsbean.setJunctionList(junctionList)
    Jsbean.setFlowConfPath(flowConfPath)
    Jsbean.setTopic(topic)
    Jsbean.setZookeeper(zookeeper)
    Jsbean.setGroup(group)
    Jsbean.setIn_table(in_table)
    Jsbean.setUrl(url)
    Jsbean.setUsername(username)
    Jsbean.setPassword(password)
    Jsbean.setOut_url(out_url)
    Jsbean.setOut_username(out_username)
    Jsbean.setOut_password(out_password)
    Jsbean.setOut_table(out_table)
    printPorperties
    Jsbean
  }

  /**
   * 打印属app属性
   */
  override def printPorperties {
    super.printPorperties
    println("============ junctionStreamingConf.xml属性一览表 ==================")
    println("streaming app 名称：" + appName)
    println("streaming 周期：" + interval)
    println("durationValue：" + durationValue)
    println("junctionList：" + junctionList)
    println("flowConfPath：" + flowConfPath)
    println("topic：" + topic)
    println("zookeeper：" + zookeeper)
    println("group：" + group)
    println("订亲表:" + in_table)
    println("读入mysql url:" + url)
    println("读入mysql username:" + username)
    println("读入mysql password:" + password)
    println("写入mysql url:" + out_url)
    println("写入mysql username:" + out_username)
    println("写入mysql password:" + out_password)
    println("写入mysql username:" + out_table)
  }
}