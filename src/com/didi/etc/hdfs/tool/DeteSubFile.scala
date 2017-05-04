package com.didi.etc.hdfs.tool
import org.apache.hadoop.fs.Path
import scala.xml.XML
import com.didi.etc.prop.DeleteSubFileProp
import com.didi.etc.its.common.util.DataFormatUtil

object DeteSubFile {

  def main(arg: Array[String]) {

    // 读取配置问件，如果指定则读入指定的配置文件，否则默认conf.xml
    var confFileName = ""
    if (arg.size == 1) confFileName = arg(0)
    val deleteSubFileProp = new DeleteSubFileProp(confFileName)
    
    // 提取配置属性
    val dsfprop = deleteSubFileProp.getDeteSubFileProp
    //打印加载的属性
    deleteSubFileProp.printPorperties

    val util = new HDFSUtil
    util.HDFSInit
    val startTime = System.currentTimeMillis
    println("开始扫描子目录......开始时间：" + DataFormatUtil.df1.format(startTime))
    // 遍历子目录列表
    util.pathList(new Path(dsfprop.getSrcDir))
    println("遍历子目录列表耗时：" + ((System.currentTimeMillis - startTime) / 1000) + "秒,共检测出" + util.pathSet.size + "个合并子目录。")
    util.pathSet foreach println
    //注意必需是正则表达式
    util.delAllFile(util.pathSet, dsfprop.getFilterRegex, dsfprop.getDelet_flg)
    val endTime = System.currentTimeMillis
    println("删除不合规则文件完成，总耗时：" + ((System.currentTimeMillis - startTime) / 1000) + "秒。")
  }
}