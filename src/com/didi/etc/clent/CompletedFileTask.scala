package com.didi.etc.clent

import java.util.concurrent.ExecutorService
import java.util.concurrent.FutureTask
import java.util.concurrent.LinkedBlockingQueue

import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.didi.etc.PropertyBean
import com.didi.etc.LoadProperties

/**
 * @author 宿荣全
 * @date 2016.11.28
 * <p>读取文件分发到hdfs上去</p>
 * <p>Tuple2[Int, String]]：Tuple2—1: 1:正常读取并上传HDFS文件，2:读取时报异常文件</p>
 */
class CompletedFileTask( delFileQueue: LinkedBlockingQueue[Tuple2[Int, String]]) extends Runnable {

  val HDFSFileSytem = LoadProperties.HDFSFileSytem
  val client = LoadProperties.propertyBean.getClientBean
  val mvDir = client.getMvSrcDir

  override def run() {
    var index =0
    val startTime = System.currentTimeMillis
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd HH:mm:ss")
    val start = dateFormat.format(startTime)
    while (true) {
      val tupleFile = delFileQueue.take
      val typeId = tupleFile._1
      val srcFile = tupleFile._2
      val hdfsPath = new Path(srcFile)
      if (isFileExists(hdfsPath)) {
        // 取扫描后的文件名 例如：“ＸＸＸＸ.scan”
        val fileName = srcFile.split("/").lastOption.get
        //去除后缀“.scan”的文件名
        val rename = fileName.substring(0, fileName.indexOf(".scan"))
        typeId match {
          case 1 => {
            HDFSFileSytem.rename(hdfsPath, new Path(mvDir + "/" + rename))
             println("CompletedFileTask From HDFSfile:" +srcFile + " To HDFSfile:"+ mvDir + "/" + rename)
          }
          case 2 => {
            if (HDFSFileSytem.rename(hdfsPath, new Path(srcFile.substring(0, srcFile.indexOf(".scan")))))
              println("读、写以及拆分hdfs源文件[" + srcFile + "]时发生错误，重新改名为[" + rename + "]后续会被再次扫描读入！")
          }
          case _ => println("没有此类型文件type=" + typeId)
        }
      }
      index = index +1
      val endTime = System.currentTimeMillis
      val subTime = (endTime-startTime)/1000
      println("从"+ start + " 〜 " + dateFormat.format(System.currentTimeMillis) + "共耗时："+ subTime +"秒，共处理：" + index + "个文件。")
    }
  }

  /**
   * 判断此文件或目录在ＨＤＦＳ上是否存在
   */
  def isFileExists(path: Path) = HDFSFileSytem.exists(path)
}