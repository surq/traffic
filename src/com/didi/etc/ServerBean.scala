package com.didi.etc

import java.net.Socket
import scala.beans.BeanProperty

class ServerBean {
  @BeanProperty
  var sourceDir = ""
  @BeanProperty
  var serverIP = ""
  @BeanProperty
  var serverPort: Int = 2016
  @BeanProperty
  var scanInterval: Long = 2000
  @BeanProperty
  var fileNameRegex = ""
  @BeanProperty
  var filterFlg: Boolean = false
}