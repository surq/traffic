package com.didi.etc.prop.bean

import scala.beans.BeanProperty

/**
 * @author 宿荣全
 * @data 2017.1.6
 * 配置文件中的OrderGPS属性
 */
class OrderGPSBean extends BasePropBean with Serializable {
  @BeanProperty var hiveDatabase = ""
  @BeanProperty var select = ""
  @BeanProperty var where = ""
  @BeanProperty var orderTable = ""
  @BeanProperty var gpsDir = ""
  @BeanProperty var resultDir = ""
  @BeanProperty var appName = ""
  @BeanProperty var areaID = ""
  @BeanProperty var ResutlUnionPathionFlg:Boolean = false
}