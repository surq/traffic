package com.didi.etc
import scala.beans.BeanProperty

 class PropertyBean {
  @BeanProperty var types =""
  @BeanProperty var serverBean: ServerBean = null
  @BeanProperty var clientBean: ClientBean = null
}