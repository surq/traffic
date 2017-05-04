package com.didi.util.auto
import scala.reflect.BeanProperty

 class DataBean {

  @BeanProperty var dataName: String = ""
  @BeanProperty var outPutPath: String = ""
  @BeanProperty var outPutFileDel_flg: Boolean = false
  @BeanProperty var lineCount: Long = 0l
  @BeanProperty var interval: Long = 0l
}