package com.didi.etc.prop.bean

import scala.beans.BeanProperty
class LngLatBean extends Serializable {

  @BeanProperty var lng_latList: Array[Array[Double]] = null
  @BeanProperty var PolygonID = ""
}
