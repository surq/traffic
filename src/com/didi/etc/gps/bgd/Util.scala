package com.didi.etc.gps.bgd
import scala.xml.XML
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration


object Util {

  // 时间格式
  val dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmm")
  val gpsdateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  /**
   * 属性文件配置
   */
  def analysis = {
    val jarName = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()
    val fileseparator = System.getProperty("file.separator")
    val jarpath = jarName.substring(0, jarName.lastIndexOf(fileseparator))
    val xmlFile = XML.load(jarpath + "/../conf/conf.xml")
    val Property = (xmlFile \ "Property")
    //    sparkMaster = (Property \ "SparkMaster").text
    val appName = (Property \ "AppName").text
    val mapmatchDir = (Property \ "MapmatchDir").text
    val junctiontext = (Property \ "JunctionList").text
    val area = (Property \ "Area").text.toFloat
    val duration = (Property \ "Duration").text.toInt
    val countCarDir = (Property \ "CountCarDir").text
    val countUserDir = (Property \ "CountUserDir").text

    val junctionList = junctiontext.split("@").map { xy =>
      {
        val points = xy.split(",")
        (points(0).toFloat, points(1).toFloat)
      }
    }
    val junctionPoints = junctionList.zipWithIndex
    val propBean = new propertyBean
    propBean.setAppName(appName)
    propBean.setMapmatchDir(mapmatchDir)
    propBean.setArea(area)
    propBean.setDuration(duration)
    propBean.setJunctionPoints(junctionPoints)
    propBean.setCountCarDir(countCarDir)
    propBean.setCountUserDir(countUserDir)

    println("appName:" + appName)
    println("mapmatchDir:" + mapmatchDir)
    println("area:" + area)
    println("duration:" + duration)
    println("junctionList:")
    junctionList foreach println
    propBean
  }

  /**
   * 判断GPS的记录是否属于路口列表中的一个路口，若属于则返回路口的index,否则返回None
   */
  def getJunction(junctionInfoList: Array[((Float, Float), Int)], points: Tuple2[Float, Float], area: Float) = {
    val indexList = for (junction <- junctionInfoList if (isJunction(junction._1, points, area))) yield junction._2
    // 一个点只能属于一个路口
    if (indexList.size > 0) Some(indexList(0)) else None
  }

  /**
   * 判断GPS的记录是否属于路口列表中的一个路口，若属于则返回路口和gps点的经纬度，否则返回None
   */
  def getJunctionPoints(junctionList: Array[(Float, Float)], points: Tuple2[Float, Float], area: Float) = {
    val resultList = for { junction <- junctionList if (isJunction(junction, points, area)) } yield (junction, points)
    // 一个点只能属于一个路口
    if (resultList.size > 0) Some(resultList(0)) else None
  }

  /**
   * 判断GPS的记录是否属于一个路口
   */
  def isJunction(junction: Tuple2[Float, Float], points: Tuple2[Float, Float], area: Float) = {
    val lat = points._1
    val lng = points._2
    val minX = lat - area
    val minY = lng - area
    val maxX = lat + area
    val maxY = lng + area
    if (minX < junction._1 && junction._1 < maxX && minY < junction._2 && junction._2 < maxY) true else false
  }
  //--------------------------------订单gps----------------------------------------------
  import org.json4s.DefaultFormats
  import org.json4s.JsonAST.{ JString, JValue }
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  import scala.collection.mutable.ArrayBuffer
  def jsonStr2ArrTuple2(jsonStr: String, fields: Array[String]): Array[(String, String)] = {
    val result = ArrayBuffer[(String, String)]()
    for (field <- fields) {
      val jsonStr_target = compact(parse(jsonStr) \ field)
      val prop = parse(jsonStr_target) match {
        case JString(str) => (field, str)
        case _            => (field, null)
      }

      if (prop._2 != null) result.append(prop)
    }
    result.toArray
  }

  def jsonStr2valueList(jsonStr: String, fields: Array[String]): Array[String] = for (field <- fields) yield compact(parse(jsonStr) \ field)

  import java.math.BigInteger;
  import java.security.MessageDigest;
  import java.security.NoSuchAlgorithmException;
  def getMD5(value: String) = new BigInteger(1, MessageDigest.getInstance("MD5").digest(value.getBytes())).toString(16)
  
  //--------------------------------------------------

//  var HDFSFileSytem: FileSystem = null
//  import scala.collection.mutable.Set
//   val pathSet = Set[Path]()
//  /**
//   * 获取hdfs
//   */
////  def getHdfs= {
////    val conf = new Configuration
////    HDFSFileSytem = FileSystem.get(conf)
////  }

  /**
   * 递归遍历要合并的所有子目录
   */
//  def pathList(path: Path) {
//    if (HDFSFileSytem.isDirectory(path)) {
//      val listStatus = HDFSFileSytem.listStatus(path).map { fileInfo => fileInfo.getPath }
//      var flg = false
//      listStatus.foreach { subpath =>
//        {
//          if (HDFSFileSytem.isDirectory(subpath)) pathList(subpath) else {
//            if (!flg) {
//              pathSet += path
//              flg = true
//            }
//          }
//        }
//      }
//    }
//  }

//  def main(args: Array[String]): Unit = {
//    val itemsArray = Array("driver_id", "begin_charge_time", "finish_time")
//    val json = """{"driver_id":566397083194898,"begin_charge_time":"2016-12-01 22:14:14","finish_time":"2016-12-01 22:26:54"}"""
//    val array = jsonStr2valueList(json, itemsArray)
//    Console println array.mkString(",")
//    
//    Console println gpsdateFormat.format(1474473642000l)
//    
//    
//    Console println getMD5("563763413254145")
//    Console println getMD5("564862529901195")
//     Console println getMD5("565177226110906")
//    Console println getMD5("563401597652992")
//  }

}