package com.didi.etc

import scala.xml.XML
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

//import com.didi.etc.PropertyBean

/**
 * @author 宿荣全
 * @date 2016.11.28
 * <p></p>
 */
object LoadProperties {

  def main(agrs: Array[String]) {
    getConfProerties
  }

  var HDFSFileSytem: FileSystem = null
  var propertyBean: PropertyBean = null
  def initPro {
    propertyBean = getConfProerties
    val conf = new Configuration
    HDFSFileSytem = FileSystem.get(conf)
  }

  /**
   * 提取配置文件信息
   */
  def getConfProerties: PropertyBean = {

    val propertyBean = new PropertyBean
    val serverBean = new ServerBean
    val clientBean = new ClientBean

    val jarName = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()
    val fileseparator = System.getProperty("file.separator")
    val jarpath = jarName.substring(0, jarName.lastIndexOf(fileseparator))
    val xmlFile = XML.load(jarpath + "/../conf/ETLconf.xml")

    //    val xmlFile = XML.load("/Users/didi/work/scala-SDK-3.0.3-2.10/testdata" + "/conf/ETLconf.xml")
    val Property = (xmlFile \ "Property")
    val types = (Property \ "Type").text
    val server = (Property \ "Server")

    val sourceDir = (server \ "SourceDir").text
    val serverIP = (server \ "ServerIP").text
    val serverPort = (server \ "ServerPort").text.toInt
    val scanInterval = (server \ "ScanInterval").text.toLong
    val fileNameRegex = (server \ "FileNameRegex").text
    val filterFlg = (server \ "FilterFlg").text.toBoolean

    //----------------------------------------------
    val client = (Property \ "Client")
    val processThreads = (client \ "ProcessThreads").text.toInt
    val desDir = (client \ "DesDir").text
    val mvSrcDir = (client \ "MvSrcDir").text
    val charset = (client \ "Charset").text
    val fildItems = (client \ "FildItems").text
    val splitKeyIndex = (client \ "SplitKeyIndex").text.toInt
    val srcSplit = (client \ "SrcSplit").text
    val desSplit = (client \ "DesSplit").text
    val aeskey = (client \ "Aeskey").text
    
    val decode_flg = (client \ "Decode_flg").text.toBoolean
    val decode_Type = (client \ "Decode_Type").text

    //----------------------------------------------
    serverBean.setSourceDir(sourceDir)
    serverBean.setServerIP(serverIP)
    serverBean.setServerPort(serverPort)
    serverBean.setScanInterval(scanInterval)
    serverBean.setFileNameRegex(fileNameRegex)
    serverBean.setFilterFlg(filterFlg)

    clientBean.setProcessThreads(processThreads)
    clientBean.setDesDir(desDir)
    clientBean.setMvSrcDir(mvSrcDir)
    clientBean.setCharset(charset)
    clientBean.setFildItems(fildItems)
    clientBean.setSplitKeyIndex(splitKeyIndex)
    clientBean.setSrcSplit(srcSplit)
    clientBean.setDesSplit(desSplit)
    clientBean.setDecode_flg(decode_flg)
    clientBean.setDecode_Type(decode_Type)
    propertyBean.setTypes(types)
    clientBean.setAeskey(aeskey)
    
    propertyBean.setServerBean(serverBean)
    propertyBean.setClientBean(clientBean)
    propertyBean
  }
}