package com.didi.etc.prop

import com.didi.etc.prop.bean.DeleteSubFileBean

class DeleteSubFileProp(confName: String) extends BaseProp(confName) with Serializable {

  var srcDir = ""
  var filterRegex = ""
  var delet_flg = true
  /**
   * 解析FileMerge属性
   */
  def getDeteSubFileProp = {
    val fileDeleFuntion = (xmlFile \ "FileDeleFuntion")
    srcDir = (fileDeleFuntion \ "SrcDir").text
    filterRegex = (fileDeleFuntion \ "FilterRegex").text
    delet_flg = (fileDeleFuntion \ "Delet_flg").text.toBoolean

    val deleteSubFileProp = new DeleteSubFileBean
    deleteSubFileProp.setSrcDir(srcDir)
    deleteSubFileProp.setFilterRegex(filterRegex)
    deleteSubFileProp.setDelet_flg(delet_flg)
    deleteSubFileProp
  }
  override def printPorperties {
    super.printPorperties
    Console println "============ DeleteSubFile属性一览表 =================="
    Console println "递规删除的根目录:" + srcDir
    Console println "文件名正则表达式:" + filterRegex
    Console println "满足表达式是否删除:" + delet_flg
  }

}