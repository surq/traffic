package com.didi.etc

import scala.beans.BeanProperty
import java.net.Socket

/**
 * @author 宿荣全
 * @date 2016.11.28
 * <p>WorkerNode 信息载体</p>
 */
class WorkerNodeBean {

  @BeanProperty
  var socket: Socket = null
  @BeanProperty
  var hostName = ""
  @BeanProperty
  var hostIp = ""
  @BeanProperty
  var workInfo = ""
  @BeanProperty
  var port = 0
}