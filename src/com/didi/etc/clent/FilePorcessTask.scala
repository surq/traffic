package com.didi.etc.clent

import java.net.InetAddress
import java.util.concurrent.LinkedBlockingQueue

import scala.io.Source

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.didi.etc.PropertyBean
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FSDataInputStream
import com.didi.etc.LoadProperties
import com.didi.etc.decode.JiNanKakouDecode

/**
 *  delFileQueue int:-->1:正常读取并上传HDFS文件，2:读取时报异常文件
 */
class FilePorcessTask(delFileQueue: LinkedBlockingQueue[Tuple2[Int, String]], srcFileName: String) extends Runnable {

  val client = LoadProperties.propertyBean.getClientBean
  val server = LoadProperties.propertyBean.getServerBean
  val HDFSFileSytem = LoadProperties.HDFSFileSytem
  val srcPath = server.getSourceDir

  // 输出目的地路径
  val desDir = client.getDesDir
  // 读取文件编码集
  val charset = client.getCharset
  // 字段名称
  val fildItems = client.getFildItems.split(",")
  // 源文件字段分隔符
  val srcSplit = client.getSrcSplit
  // 生成的新文件分隔符
  val desSplit = client.getDesSplit
  // 拆分文件的关键字所在的index，从0开始记数
  val splitKeyIndex = client.getSplitKeyIndex
  // 是否是密文
  val decode_flg = client.getDecode_flg

  // 源hdfs的文件名字
  val fileNameWithScan = (srcFileName.split("/")).lastOption.get
  val fileName = fileNameWithScan.substring(0, fileNameWithScan.indexOf(".scan"))

  // 装载HDFS源文件数据集
  var kv_contens: List[(String, String)] = null

  override def run = writeHDFSFile(readHDFSFile)
  /**
   * 从hdfs上读取文件到list,返回list
   */
  def readHDFSFile = {
    var srcDFSInput: FSDataInputStream = null
    val srcFilePath = new Path(srcFileName)
    try {
      if (isFileExists(srcFilePath)) {
        srcDFSInput = HDFSFileSytem.open(srcFilePath)
        // 过滤掉字段个数不复的数据,返回以拆分关键字为key,line为结果的数据集（true/false,key,line ）
        var lines = Source.fromInputStream(srcDFSInput, charset).getLines
        //是否需要解密
        if (decode_flg) {
          // 解密类型
          val decode_Type = client.getDecode_Type
          //aeskey
          val aeskey = client.getAeskey
          if (decode_Type == "JiNanKakouDecode") {
            val jncode = new JiNanKakouDecode
            lines = jncode.decode(lines, aeskey,"gbk")
          }
        }
        kv_contens = lines.map(line => {
          // 行换成字段列表（最终加end字段）
          val newline = (line + srcSplit + "end").split(srcSplit)
          // (是否是有效数据flg,key,line)
          val flg = (newline.length == fildItems.length + 1)
          // 无效数据不用取key
          (flg, if (flg)keySplit(newline(splitKeyIndex))else "", line)
        }).filter(_._1).map(line => (line._2, line._3)).toList
      }
    } catch {
      // 读入异常时，把list置为null
      case e: Exception => {
        kv_contens = null
        println("读取hdfs源文件" + srcFileName + "有误，后续会被再次扫描读入！")
        e.printStackTrace
      }
    } finally {
      if (srcDFSInput != null) srcDFSInput.close
    }
    kv_contens
  }

  /**
   * 把list内容写入hdfs文件
   */
  def writeHDFSFile(lineList: List[Tuple2[String, String]]) {
    // 读文件时发生异常
    if (kv_contens == null) {
      delFileQueue.offer((2, srcFileName))
      return
    }

    // 把源文件根据key 拆分成多个小文件
    var fileIndex = 0
    var writeflg = false
    val groupContens = lineList.groupBy(_._1).foreach(subgroup => {
      val key = subgroup._1
      val list = subgroup._2
      // 拆分文件的子id
      fileIndex = fileIndex + 1
      // 拆分后文件的全路径带文件名
      val reDesFile = desDir + key + fileName + "_" + fileIndex
      //在写成过程中后缀名为“.writting”,写完后重新命名去掉此后缀
      val desFile = reDesFile + ".writting"
      val newpathFile = new Path(desFile)
      var outputDFS: FSDataOutputStream = null
      try {
        // 拆分后文件的全路径
        val dirPath = new Path(desFile.substring(0, desFile.lastIndexOf("/")))
        if (!HDFSFileSytem.isDirectory(dirPath)) HDFSFileSytem.mkdirs(dirPath)

        // 上一次写入时出错遗留下的文件
        if (isFileExists(newpathFile)) HDFSFileSytem.delete(newpathFile, false)
        outputDFS = HDFSFileSytem.create(newpathFile)
        val separator = System.getProperty("line.separator")
        list.foreach(kv => {
          outputDFS.writeBytes(kv._2)
          outputDFS.writeBytes(separator)
          outputDFS.flush()
        })

      } catch {
        case e: Exception => {
          // 写小文件时有误需要重写
          writeflg = true
          e.printStackTrace
        }
      } finally {
    	  HDFSFileSytem.rename(newpathFile, new Path(reDesFile))
        if (outputDFS != null) outputDFS.close
      }
    })
    // 在写小文件时有异常的情况 2:重要重新写入
    if (writeflg) {
      delFileQueue.offer((2, srcFileName))
      println("拆分文件［" + srcFileName + "］时有误！")
    } // 所拆分小文件正常全部写入的情况
    else delFileQueue.offer((1, srcFileName))
    println("FilePorcessTask porcessed File;" + srcFileName)
  }

  /**
   * 格式拆分：2016-09-20 23:03:10 140 => /2016/09/20/23/
   */
  def keySplit(key: String) = {
    val separator = System.getProperty("file.separator")
    val list = key.split(" ")

    val date = list(0).split("-")
    val time = list(1).split(":")
    val result = (date ++ time.take(1))
    result.mkString(separator, separator, separator)
  }

  //  def getSerNum = (new util.Random).nextInt(10000)

  /**
   * 判断此文件或目录在ＨＤＦＳ上是否存在
   */
  def isFileExists(path: Path) = HDFSFileSytem.exists(path)

}