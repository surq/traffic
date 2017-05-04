package com.didi.util.auto

import java.util.Calendar
import java.util.regex.Pattern
import scala.xml.XML
import org.apache.hadoop.fs.FileSystem
import scala.io.Source
import com.didi.etc.prop.bean.AutoSysConfBean
import com.didi.etc.prop.bean.AutoConfBean
import com.didi.etc.hdfs.tool.HDFSUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataInputStream
import com.didi.etc.its.common.util.DataFormatUtil

/**
 * 自带日期参数yyyyMMddHH
 */
class OrderGPSAutoUtil(date: String) {

  val calendar = Calendar.getInstance

  // 指定期数据
  val yearTmp = date.substring(0, 4)
  val monthTmp = date.substring(4, 6)
  val dayTmp = date.substring(6, 8)

  calendar.set(Calendar.YEAR, yearTmp.toInt)
  calendar.set(Calendar.MONTH, monthTmp.toInt - 1)
  calendar.set(Calendar.DAY_OF_MONTH, dayTmp.toInt)
  // 开始时间
  val starttime = calendar.getTime
  val data_time = calendar.getTimeInMillis
  val startDate = DataFormatUtil.date_YMD_1.format(starttime)
  //所处理的gps轨迹内容时间
  val gpsDate = DataFormatUtil.date_YMD_2.format(starttime)
  val year = DataFormatUtil.date_Y.format(data_time)
  val month = DataFormatUtil.date_M.format(data_time)

  // 截止日期
  calendar.add(Calendar.DATE, 1)
  val time = calendar.getTimeInMillis
  val endDate = DataFormatUtil.date_YMD_1.format(time)
  //  val year = DataFormatUtil.date_Y.format(time)
  //  val month = DataFormatUtil.date_M.format(time)

  val lineseparator = System.getProperty("line.separator")

  /**
   * 自动化脚本xml属性解析
   */
  def anlysisProp(autoConf: String) = {
    val xmlFile = XML.load(autoConf)
    val autoScript = (xmlFile \ "AutoScript")
    val taskQueue = (autoScript \ "TaskQueue").text
    val sysConfPath = (autoScript \ "SysConfPath").text
    val saprkParam_templete = (autoScript \ "SaprkParam_templete").text
    val taxiXml_templete = (autoScript \ "TaxiXml_templete").text
    val gulfXml_templete = (autoScript \ "GulfXml_templete").text
    val xmlOutPath = (autoScript \ "XmlOutPath").text
    val orderGPSHadoopUser = (autoScript \ "OrderGPSHadoopUser").text
    val orderShellPath = (autoScript \ "OrderShellPath").text
    val polygonHadoopUser = (autoScript \ "PolygonHadoopUser").text
    val polygonShellPath = (autoScript \ "PolygonShellPath").text
    val polygonjarPath = (autoScript \ "PolygonjarPath").text

    val autoConfBean = new AutoConfBean
    autoConfBean.setTaskQueue(taskQueue)
    autoConfBean.setSysConfPath(sysConfPath)
    autoConfBean.setSaprkParam_templete(saprkParam_templete)
    autoConfBean.setTaxiXml_templete(taxiXml_templete)
    autoConfBean.setGulfXml_templete(gulfXml_templete)
    autoConfBean.setXmlOutPath(xmlOutPath)
    autoConfBean.setOrderGPSHadoopUser(orderGPSHadoopUser)
    autoConfBean.setOrderShellPath(orderShellPath)
    autoConfBean
  }

  /**
   * 生成xml文件和shell脚本文件
   */
  def cretateXMLAndSH(xmlTempPath: String,
                      autoConfBean: AutoConfBean,
                      sysConfList: List[AutoSysConfBean],
                      list: List[String]) = {
    sysConfList.map { bean =>
      {
        val linesList = getFileLines(xmlTempPath)
        val contStr = linesList.mkString(lineseparator)
        // 取出模板中的key-value
        val kvList = getValuesList(list, contStr)
        // xml类型
        val xmlType = (kvList.toMap).get("Type").get
        val values = modifyFun(xmlType, kvList, bean)
        val appName = values.toMap.get("AppName").get
        val xmlName = appName + ".xml"
        val xmlFilePath = autoConfBean.getXmlOutPath + "/" + xmlName
        val newcontStr = replaceValues(values, contStr)
        Console println newcontStr
        // 生成taxi的xml的属性配置文件
        writeFile(xmlFilePath, newcontStr)

        val sparkTaskShStr = getFileLines(autoConfBean.getSaprkParam_templete).mkString(lineseparator)
        val shScreapt = getMakeComand(sparkTaskShStr, autoConfBean, xmlName, appName + ".out")
        Console println shScreapt
        val shellPath = autoConfBean.getOrderShellPath + "/" + appName + ".sh"
        writeFile(shellPath, shScreapt)
        shellPath
      }
    }
  }

  /**
   * 生成后台执行脚本
   */
  def mergerShell(shellList: List[String], filePath: String) = {
    // ----------串行计算----------
    //合并所有单独的脚本,集合到一个脚本中去
    // val shell = shellList.mkString("""#!/bin/bash""" + lineseparator + "sh ", lineseparator + "sh ", lineseparator)
    //--------并行计算-----------
    val splitn1 = " 2>&1 &" + lineseparator + "nohup sh "
    val text = shellList.mkString("""#!/bin/bash""" + lineseparator + "nohup sh ", splitn1, " 2>&1 &")
    val shell = text + lineseparator + "wait" + lineseparator

    // 生成autoPolygon总的自动化脚本
    writeFile(filePath, shell)
  }

  /**
   * 修改xml模板中的条件
   */
  def modifyFun(xmlType: String, kvList: List[(String, String)], bean: AutoSysConfBean) = {

    kvList.map(kv => {
      val key = kv._1
      val value = kv._2
      key match {
        case "Where"     => (key, whereAction(value, bean))
        case "GPSDir"    => (key, bean.getPolygonGps_Path + "/" + xmlType + gpsDate)
        case "AreaID"    => (key, bean.getPolygonID)
        case "ResultDir" => (key, bean.getOrderGps_Path + "/" + xmlType)
        case "AppName"   => (key, bean.getTaskName + value)
        case _           => (key, value)
      }
    })
  }

  /**
   * gulf,taxi xml模板中where条件的处理
   */
  def whereAction(str: String, bean: AutoSysConfBean) = {

    val sqlList = str.split("@")
    // city_id=1
    val city_id = sqlList(0).split("=")
    city_id(1) = bean.getCityID
    sqlList(0) = city_id.mkString("=")
    val timeTuples = getIntervalDate
    // 2017-01-13
    sqlList(2) = timeTuples._1
    // 2017-01-16
    sqlList(4) = timeTuples._2
    // 2017
    sqlList(6) = timeTuples._3
    // 01
    sqlList(8) = timeTuples._4
    sqlList.mkString
  }

  def replaceValues(values: List[(String, String)], contStr: String) = {
    var str = contStr
    values.foreach(key => (key, str = replaceValue(key, str)))
    str
  }
  /**
   * 根据外部配置替换xml模板的属性
   */
  def replaceValue(kv: Tuple2[String, String], contStr: String) = {
    val key = kv._1
    val replaceValue = kv._2
    contStr.replaceAll("<" + key + ">(.*)</" + key + ">", "<" + key + ">" + replaceValue + "</" + key + ">")
  }

  /**
   * 获模板中keys所对应的values
   */
  def getValuesList(keyList: List[String], contString: String) = keyList.map(key => (key, getValue(key, contString)))

  /**
   * 获模板中key所对应的value
   */
  def getValue(key: String, contStr: String) = {
    val matcher = Pattern.compile("<" + key + ">(.*)</" + key + ">").matcher(contStr)
    if (matcher.find) matcher.group(1) else ""
  }

  /**
   * 获取查询条件的开始结束时间
   */
  def getIntervalDate = (startDate, endDate, year, month)

  /**
   * test用
   * 读取自动化的基本配置
   */
  def readConf(confName: String) = {
    val jarName = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()
    val fileseparator = System.getProperty("file.separator")
    val jarpath = jarName.substring(0, jarName.lastIndexOf(fileseparator))
    val confPath = "/Users/didi/work/taxi_conf_tmp.xml"
    val xmlFile = XML.load(confPath)
    xmlFile
  }

  /**
   * 读取HDFS自动化系统配置
   */
  def getHDFSFileConf(confPath: String) = {
    val hdfsutil = new HDFSUtil
    hdfsutil.HDFSInit
    var HDFSFileSytem = hdfsutil.HDFSFileSytem
    val path = new Path(confPath)

    if (HDFSFileSytem.exists(path)) {
      val srcDFSInput = HDFSFileSytem.open(path)
      //过滤掉配置文件中以"#"开头的注释文件
      val line_content = getHdfsFileLines(srcDFSInput).map(_.trim).filter { line => (!line.startsWith("#") && line.trim != "") }

      // 获取autoSysConf配置有效项生成本地临时文件autoSysConf.tmp,供多边形读取
      writeFile("./autoConf/autoSysConf.tmp", gpsDate + lineseparator + line_content.mkString(lineseparator) + lineseparator)

      val lines = line_content.tail.map(_.split(","))
      lines.map { propList =>
        {
          //任务名称,MM_PATH,多边形区域,多边形ID,城市ID,城市名称,多边形区域GPS_PATH,订单GPS_PATH
          val bean = new AutoSysConfBean
          bean.setTaskName(propList(0))
          bean.setMm_Path(propList(1))
          bean.setPolygon(propList(2))
          bean.setPolygonID(propList(3))
          bean.setCityID(propList(4))
          bean.setCityName(propList(5))
          bean.setPolygonGps_Path(propList(6))
          //订单GPS_PATH/城市名称/任务名称
          bean.setOrderGps_Path(propList(7) + "/" + propList(5) + "/" + propList(0))
          bean
        }
      }.toList
    } else List[AutoSysConfBean]()
  }

  /**
   * 生成脚本内容
   */
  def getMakeComand(sparkTaskShStr: String, autoConfBean: AutoConfBean, xmlName: String, logFileName: String) =
    sparkTaskShStr + xmlName + " \\" + lineseparator + "--class com.didi.etc.its.tool.OrderGPS " +
      "$HOME/lib/diditool.jar \\" + lineseparator + xmlName + """ > $HOME/log/""" + logFileName

  /**
   * 写本地文件
   */
  def writeFile(outputPath: String, str: String) = {
    import java.io._
    val writer = new PrintWriter(new File(outputPath), "UTF-8")
    writer.write(str)
    writer.close
  }
  /**
   * 读取本地文件
   */
  def getFileLines(fileName: String) = Source.fromFile(fileName, "UTF-8").getLines.toList
  /**
   * 读取hdfs文件
   */
  def getHdfsFileLines(inputStream: FSDataInputStream) = Source.fromInputStream(inputStream, "UTF-8").getLines.toList
}