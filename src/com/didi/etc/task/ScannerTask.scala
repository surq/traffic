package com.didi.etc.task

import java.util.concurrent.LinkedBlockingQueue

import com.didi.etc.LoadProperties
import org.apache.hadoop.fs.Path

/**
 * @author 宿荣全
 * @since 2016.12.06
 * HDFS文件扫描功能，扫描完的文件重命名，加后缀名“.scan”
 */
class ScannerTask(newFileQueue: LinkedBlockingQueue[String]) extends Runnable {

  val server = LoadProperties.propertyBean.getServerBean
  val scanDir = server.getSourceDir

  // hdfs 文件过滤规则
  val filterRegex = server.getFileNameRegex
  val filterFlg = server.getFilterFlg

  /**
   * 线程工作：周期性的扫描hdfs文件列表
   */
  override def run() {
    val HDFSFileSytem = LoadProperties.HDFSFileSytem
    while (true) {
      try {
        val listStatus = HDFSFileSytem.listStatus(new Path(scanDir))
        listStatus.foreach(file => {

          val hdfsFile = file.getPath
          // 加入文件过滤规则，对正在写入的文件过滤
          if (fileNameFilter(hdfsFile.getName)) {
            // 对扫描到的文件加后缀标识
            val sanFileName = hdfsFile.toString + ".scan"
            // 重新命名
            val flg = HDFSFileSytem.rename(hdfsFile, new Path(sanFileName))
            println("HDFS扫描文件：" + sanFileName)
            if (flg) newFileQueue.offer(sanFileName)
          }

        })
        Thread.sleep(server.getScanInterval)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  /**
   * 按配置参数FilterRegex、FilterFlg匹配文件
   */
  def fileNameFilter(fileName: String) = {
    if (defaultFilter(fileName)) {
      filterFlg match {
        case true  => isMatch(fileName)
        case false => !isMatch(fileName)
      }
    } else false
  }

  /**
   * 用户指定的文件匹配规则
   */
  def isMatch(fileName: String) = fileName.matches(filterRegex)

  /**
   * 默认扫描规则
   */
  def defaultFilter(fileName: String) = {
    // flume 正在写入的文件
    if (fileName.endsWith(".tmp")) false
    // 已扫描过的文件
    else if (fileName.endsWith(".scan")) false
    // hdfs 正在拷入的文件
    else if (fileName.endsWith("._COPYING_")) false
    else true
  }
}