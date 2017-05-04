package com.didi.etc.prop.bean

import scala.beans.BeanProperty
class FileMergeBean extends BasePropBean with Serializable {
    @BeanProperty var SrcDir = ""
}