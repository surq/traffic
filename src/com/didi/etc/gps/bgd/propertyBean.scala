package com.didi.etc.gps.bgd

import scala.beans.BeanProperty
/**
 * @author 宿荣全
 */
class propertyBean extends Serializable{
  @BeanProperty var appName = ""
  @BeanProperty var mapmatchDir = ""
  @BeanProperty var area: Float = 0
  @BeanProperty var duration: Int = 0
  @BeanProperty var countCarDir = ""
  @BeanProperty var countUserDir =""
  @BeanProperty var junctionPoints: Array[((Float, Float), Int)] = null
}