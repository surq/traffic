package com.didi.etc.prop.bean

import scala.beans.BeanProperty
/**
 * 自动化工具本身要读的配置
 */
class AutoConfBean {

  @BeanProperty var taskQueue = ""
  @BeanProperty var sysConfPath = ""
  @BeanProperty var saprkParam_templete = ""
  @BeanProperty var taxiXml_templete = ""
  @BeanProperty var gulfXml_templete = ""
  @BeanProperty var xmlOutPath = ""
   @BeanProperty var orderGPSHadoopUser = ""
  @BeanProperty var orderShellPath = ""
}