package com.didi.etc.prop.bean

import scala.beans.BeanProperty
class PolygonUserConfBean extends Serializable{
  @BeanProperty var polygonID = ""
  @BeanProperty var polygonCity = ""
  @BeanProperty var polygonTaskName = ""
  @BeanProperty var outPutDir = ""
  @BeanProperty var plygonArray: Array[Array[Double]] = null
  //-------xml配置 spark------------
  @BeanProperty var userConfDir = ""
  @BeanProperty var appName = ""
  @BeanProperty var interval = 2000
  //-------xml配置 kafka------------
  @BeanProperty var directAPI = false
  @BeanProperty var topic = ""
  @BeanProperty var brokerList = ""
  @BeanProperty var zookeeper = ""
  @BeanProperty var group = ""
  @BeanProperty var receverNum = 1
  @BeanProperty var outKafkaTopic =""
}