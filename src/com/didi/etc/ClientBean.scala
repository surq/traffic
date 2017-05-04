package com.didi.etc
import scala.beans.BeanProperty

class ClientBean {
  @BeanProperty
  var processThreads: Int = 1
  @BeanProperty
  var desDir = ""
  @BeanProperty
  var charset = "UTF-8"
  @BeanProperty
  var fildItems = ""
  @BeanProperty
  var srcSplit = ""
  @BeanProperty
  var desSplit = ""
  @BeanProperty
  var SplitKeyIndex: Int = 0
  @BeanProperty
  var mvSrcDir = ""
  @BeanProperty
  var decode_flg: Boolean = false
  @BeanProperty
  var decode_Type = ""
  @BeanProperty
  var aeskey = ""
}