package com.didi.etc.prop

import com.didi.etc.prop.bean.FileMergeBean

class FileMergeProp(confName: String) extends BaseProp(confName) with Serializable {

  var srcDir = ""
  /**
   * 解析FileMerge属性
   */
  def getFileMergeProp = {
    val fileMergeFuntion = (xmlFile \ "FileMergeFuntion")
    srcDir = (fileMergeFuntion \ "SrcDir").text

    val fmb = new FileMergeBean
    fmb.setSrcDir(srcDir)
    fmb
  }
  override def printPorperties {
    super.printPorperties
    Console println "============ FileMerge属性一览表 =================="
    Console println "合并路径:" + srcDir
  }
}