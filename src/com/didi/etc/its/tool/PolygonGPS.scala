package com.didi.etc.its.tool

import scala.io.Source
import com.didi.etc.prop.bean.AutoSysConfBean
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.didi.util.auto.PolygonAction

/**
 * @author 宿荣全
 * @date 20170214
 * 抽取多边形gps数据
 */
object PolygonGPS {

  def main(args: Array[String]): Unit = {

    var dgpsType = ""
    if (args.size == 1) dgpsType = args(0) else {
      Console println "请输入数据类型==专快：gulf,出租车:taxi"
      return
    }

    // 读取系统配置文件   
    val sysConfLines = getFileLines("./autoConf/autoSysConf.tmp").filter(line => (line.trim != "" && !line.startsWith("#")))
    // 获取执行的时间 /yyyy/MM/dd
    val date = sysConfLines.head

    // 封装成beanList (去除时间和标头行)
    val taskList = getSysconfBean(sysConfLines.takeRight(sysConfLines.size - 2))
    // 输入，输入相同的为一组(PolygonID,Array[座标点])
    val batchList = getPolygonList(taskList)

    dgpsType match {
      case "gulf" => new PolygonAction(batchList, date, "gulf", "biztype258_role1").run
      case "taxi" => new PolygonAction(batchList, date, "taxi", "biztype257_role1").run
      case _      => Console println "请输入的数据类型不正确，目前只支持【gulf】，【taxi】两种！"
    }
  }

  /**
   * 封装系统配置文件Bean
   */
  def getSysconfBean(sysConfLines: List[String]) = {
    sysConfLines.map(line => {
      val propList = line.split(",")
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
    }).toList
  }

  /**
   * 提取 PolygonID，Polygon列表，一次执行
   */
  def getPolygonList(beeanList: List[AutoSysConfBean]) = {

    //在输入，输出路径相同情况下，收集多个多边形区域，一次性跑完
    beeanList.map { bean => (bean.getMm_Path + "," + bean.getPolygonGps_Path, bean) }.groupBy(_._1).map(bean => {
      //(MM_PATH,多边形区域GPS_PATH)
      val key = bean._1
      val valuesList = bean._2
      // 多边形区域
      val polygonList = for (prop <- valuesList) yield (prop._2.getPolygonID, prop._2.getPolygon)
      (key, polygonList)
    }).toList
  }

  /**
   * 读取本地文件
   */
  def getFileLines(fileName: String) = Source.fromFile(fileName, "UTF-8").getLines.toList
}