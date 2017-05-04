package com.didi.etc.hdfs.tool

import com.didi.etc.prop.SplitDirAndMvFilesProp
import org.apache.hadoop.fs.Path
import com.didi.etc.its.common.util.DataFormatUtil

object SplitDirAndMvFiles {
  def main(arg: Array[String]) {

    // 读取配置问件，如果指定则读入指定的配置文件，否则默认conf.xml
    var confFileName = ""
    if (arg.size == 1) confFileName = arg(0)
    val splitDirAndMvFilesProp = new SplitDirAndMvFilesProp(confFileName)

    // 初始化属性 提取配置属性
    val sdamfProp = splitDirAndMvFilesProp.getSplitDirAndMvFilesProp

    //打印加载的属性
    splitDirAndMvFilesProp.printPorperties

    val util = new HDFSUtil
    util.HDFSInit
    val starttime = System.currentTimeMillis
    Console println "SplitDirAndMvFiles开始运行,开始时间:" + DataFormatUtil.df1.format(starttime)
    println("开始扫描子目录......")
    // 遍历子目录列表
    util.pathList(new Path(sdamfProp.getSrcDir))
    // 子目录列表一览
    val subPathSet = util.pathSet
    println("遍历子目录列表耗时：" + ((System.currentTimeMillis - starttime) / 1000) + "秒,共检测出" + subPathSet.size + "个子目录。")
    subPathSet.map { x => { println("子目录(下一层有文件)列表:" + x.toString); x.toString } }.foreach(path => {

      val srcFilePath = path + "/" + sdamfProp.getSrcFileName
      val desPath = sdamfProp.getDesDir + "/" + path.split("/").takeRight(sdamfProp.getFloor).mkString("/")
      val desFilePath = desPath + "/" + sdamfProp.getDesFileName
      // 判断目标目录是否存在，若不存在则创建
      if (!util.HDFSFileSytem.isDirectory(new Path(desPath))) util.HDFSFileSytem.mkdirs(new Path(desPath))
      val flg = util.HDFSFileSytem.rename(new Path(srcFilePath), new Path(desFilePath))
      if (flg) Console println srcFilePath + " 成功转移到==> " + desFilePath
    })

    val end = System.currentTimeMillis
    Console println "SplitDirAndMvFiles运行完毕,结束时间:" + DataFormatUtil.df1.format(end)
    println("任务完成总耗时：" + ((end - starttime) / 1000) + "秒。")
  }
}