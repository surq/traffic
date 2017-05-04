package com.didi.etc.prop.bean
import scala.beans.BeanProperty

class DeleteSubFileBean extends BasePropBean with Serializable {
  @BeanProperty var SrcDir = ""
  @BeanProperty var filterRegex = ""
  @BeanProperty var delet_flg = true
}