package com.didi.etc.hdfs.tool

import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.Set
import scala.xml.XML
import com.didi.etc.LoadProperties
import com.didi.etc.prop.BaseProp
import com.didi.etc.prop.FileMergeProp
import com.didi.etc.its.common.util.DataFormatUtil

object FilesMeger {

  def main(arg: Array[String]) {

    // 读取配置问件，如果指定则读入指定的配置文件，否则默认conf.xml
    var confFileName = ""
    if (arg.size == 1) confFileName = arg(0)
    val fileMergeProp = new FileMergeProp(confFileName)

    // 提取配置属性
    val srcDir = fileMergeProp.getFileMergeProp.getSrcDir

    //打印加载的属性
    fileMergeProp.printPorperties

    val util = new HDFSUtil
    util.HDFSInit
    val starttime = System.currentTimeMillis
    Console println "FilesMeger开始运行,开始时间:" + DataFormatUtil.df1.format(starttime)
    println("开始扫描子目录......")
    // 遍历子目录列表
    util.pathList(new Path(srcDir))
    // 子目录列表一览
    val subPathSet = util.pathSet
    println("遍历子目录列表耗时：" + ((System.currentTimeMillis - starttime) / 1000) + "秒,共检测出" + subPathSet.size + "个合并子目录。")
    subPathSet.foreach { x => println("所合并的子目录列表:" + x.toString) }

    // 遍历所有的子目录进行合并
    util.unionAllFile(subPathSet)
    val end = System.currentTimeMillis
    Console println "FilesMeger运行完毕,结束时间:" + DataFormatUtil.df1.format(end)
    println("文件合并全部完成，总耗时：" + ((end - starttime) / 1000) + "秒。")
  }
}