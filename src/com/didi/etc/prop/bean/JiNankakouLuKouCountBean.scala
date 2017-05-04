package com.didi.etc.prop.bean

import scala.beans.BeanProperty
class JiNankakouLuKouCountBean extends Serializable {
  @BeanProperty var cityId = ""
  @BeanProperty var kakouItemList: Array[String] = Array[String]()
  @BeanProperty var countInterval =5
  @BeanProperty var appName = ""
  @BeanProperty var kakouDescribeList : Array[String] = Array[String]()
  @BeanProperty var kakouIdList: Array[String] = Array[String]()
  @BeanProperty var batchPattern = ""
  @BeanProperty var kakouPath = ""
  @BeanProperty var out_url = ""
  @BeanProperty var out_username = ""
  @BeanProperty var out_password = ""
  @BeanProperty var out_table = ""
}