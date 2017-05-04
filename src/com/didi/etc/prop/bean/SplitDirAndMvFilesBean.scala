package com.didi.etc.prop.bean

import scala.beans.BeanProperty

class SplitDirAndMvFilesBean extends BasePropBean with Serializable {
  @BeanProperty var srcDir = ""
  @BeanProperty var desDir = ""
  @BeanProperty var floor = 0
  @BeanProperty var srcFileName = ""
  @BeanProperty var desFileName = ""
}