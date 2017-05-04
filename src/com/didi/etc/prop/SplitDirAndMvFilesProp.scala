package com.didi.etc.prop

import com.didi.etc.prop.bean.SplitDirAndMvFilesBean

class SplitDirAndMvFilesProp(confName: String) extends BaseProp(confName) with Serializable {

  var srcDir = ""
  var desDir = ""
  var floor = 0
  var srcFileName = ""
  var desFileName = ""

  /**
   * 解析FileMerge属性
   */
  def getSplitDirAndMvFilesProp = {
    val splitDirAndMvFilesFuntion = (xmlFile \ "SplitDirAndMvFilesFuntion")
    srcDir = (splitDirAndMvFilesFuntion \ "SrcDir").text
    desDir = (splitDirAndMvFilesFuntion \ "DesDir").text
    floor = (splitDirAndMvFilesFuntion \ "Floor").text.toInt
    srcFileName = (splitDirAndMvFilesFuntion \ "SrcFileName").text
    desFileName = (splitDirAndMvFilesFuntion \ "DesFileName").text
    val sdamf = new SplitDirAndMvFilesBean
    sdamf.setSrcDir(srcDir)
    sdamf.setDesDir(desDir)
    sdamf.setFloor(floor)
    sdamf.setSrcFileName(srcFileName)
    sdamf.setDesFileName(desFileName)
    sdamf
  }
  override def printPorperties {
    super.printPorperties
    Console println "======= SplitDirAndMvFiles属性一览表 ==========="
    Console println "拆断截取的根目录:" + srcDir
    Console println "转移拼接根目录:" + desDir
    Console println "从右截取的层数：" + floor
    Console println "转移的文件名：" + srcFileName
    Console println "转移后的文件名：" + desFileName
  }
}