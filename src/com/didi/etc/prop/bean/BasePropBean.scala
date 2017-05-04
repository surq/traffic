package com.didi.etc.prop.bean

import scala.beans.BeanProperty
/**
 * @author 宿荣全
 * @data 2017.1.6
 * 配置文件中的基本属性
 */
class BasePropBean extends Serializable {
  @BeanProperty var jarName = ""
  @BeanProperty var confPath = ""
  @BeanProperty var hDFSName = ""
  @BeanProperty var clusterUrl = ""
}