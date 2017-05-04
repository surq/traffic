package com.didi.util.auto

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.didi.etc.prop.bean.LngLatBean
import com.didi.etc.hdfs.tool.HDFSUtil
import org.apache.hadoop.fs.Path
import com.didi.etc.its.common.util.DataFormatUtil

class PolygonAction(
    batchList: List[(String, List[(String, String)])],
    date: String, // 抽取指定时间内的轨迹数据
    gpsType: String, // gulf or taxi
    fileType: String // gulf数据为：biztype258_role1，taxi数据为：biztype257_role1
    ) {
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /**
   * 线程工作：周期性的扫描hdfs文件列表
   */
  def run() {

    val sparkConf = new SparkConf
    sparkConf.setAppName("PolygonGPS_" + gpsType)
    val sc = new SparkContext(sparkConf)
    // 00~23点
    val hour24List = (0 to 9).map("0" + _).toList ::: (10 to 23).map(_.toString).toList
    batchList.map { batch =>
      {
        val paths = (batch._1).split(",")
        //mapmatch路径
        val Mm_Path = paths(0)
        //多边形产出路径
        val PolygonGps_Path = paths(1)

        //(PolygonID,Polygon)
        val list = batch._2
        // 对每条配置任务的多边形座标进行封装
        val lngLatBeanList = getLnglatInfo(list)

        //  print INFO
        lngLatBeanList.foreach(bean => {
          print("PolygonID:" + bean.getPolygonID)
          val sst = bean.getLng_latList
          println(" 多边形定点：")
          sst.foreach { point => println(point(0) + "," + point(1)) }
        })

        // 多边形产生结果存放路径
        val outpathBase = PolygonGps_Path + "/" + gpsType + date
        //---------------删除输出结果依赖的路径---------------
        val util = new HDFSUtil
        // 获取hdfs实例HDFSFileSytem
        util.HDFSInit
        val hdfs = util.HDFSFileSytem
        val result_path = new Path(outpathBase)
        if (hdfs.exists(result_path) && hdfs.delete(result_path, true)) println("删除Polygon结果依赖路径：" + outpathBase)
        //-----------------------------------------------

        // 24小时的数据
        val succHour = hour24List.filter(hour => hdfs.exists(new Path(Mm_Path + date + "/" + hour + "/" + "_SUCCESS")))
        
        // 24小时的数据
        succHour.foreach { hour =>
          {
            val pgs_in = Mm_Path + date + "/" + hour + "/" + fileType
            val pgs_out = outpathBase + "/" + hour
            if (gpsType == "gulf")
              println(format.format(System.currentTimeMillis) + " 正在处理专快数据：" + pgs_in + " 输出到：" + pgs_out)
            else
              println(format.format(System.currentTimeMillis) + " 正在处理出租车数据：" + pgs_in + " 输出到：" + pgs_out)
            // 业务处理
            runAction(sc, pgs_in, pgs_out, lngLatBeanList)
          }
        }
      }
    }
  }

  /**
   * 多边形数据抽取
   */
  def runAction(sc: SparkContext, inPath: String, outPath: String, lngLatBeanList: List[LngLatBean]) = {
    val rdd = sc.textFile(inPath).map(_.split("\t")).filter(_.size == 4).map { items =>
      {
        val gpsList = items(3).split(";")
        val lines = gpsList.map { line =>
          {
            val dotStr = line.split(",")
            val lng = dotStr(1).toDouble
            val lat = dotStr(2).toDouble
            val idList = for (polygon <- lngLatBeanList if (GPS.inPolygon(lng, lat, polygon.getLng_latList))) yield polygon.getPolygonID
            if (idList.size > 0) Some("|" + idList(0) + "," + line) else None
          }
        }.filter(_ != None).map(_.get)
        if (lines.size > 0) Some(items.take(3).mkString("\t") + "\t" + lines.mkString(";")) else None
      }
    }.filter(_ != None).map(_.get)
    if (rdd.partitions.size > 0) rdd.saveAsTextFile(outPath)
  }

  /**
   * 封装经纬度点
   */
  def getLnglatInfo(polygonInfo: List[(String, String)]) = {
    polygonInfo.map(line => {

      val polygonId = line._1
      // 座标格式要求：座标之间用“；”隔开，经纬度间用“@”隔开。123@234;2134@12133;
      val polygon = line._2
      // 解析成座标对
      val lng_lat = polygon.split(";")
      val lng_latList = lng_lat.map { points =>
        {
          val par = points.split("@")
          // 座标精度只保留4位
          Array(DataFormatUtil.d1.format(par(0).toDouble).toDouble, DataFormatUtil.d1.format(par(1).toDouble).toDouble)
        }
      }
      val bean = new LngLatBean
      bean.setPolygonID(polygonId)
      bean.setLng_latList(lng_latList)
      bean
    })
  }
}