package com.didi.etc.prop.bean

import scala.beans.BeanProperty
class JunctionStreamingBean extends Serializable {
  @BeanProperty var appName = ""
  @BeanProperty var interval = 2000
  @BeanProperty var durationValue = 0
  @BeanProperty var junctionList = ""
  @BeanProperty var flowConfPath = ""
  @BeanProperty var topic = ""
  @BeanProperty var zookeeper = ""
  @BeanProperty var group = ""
  @BeanProperty var in_table = ""
  @BeanProperty var url = ""
  @BeanProperty var username = ""
  @BeanProperty var password = ""
  @BeanProperty var out_url = ""
  @BeanProperty var out_username = ""
  @BeanProperty var out_password = ""
  @BeanProperty var out_table = ""
}