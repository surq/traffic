package com.didi.etc.its.tool

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.didi.etc.prop.bean.OrderGPSBean
import com.didi.etc.prop.OrderGPSProp
import com.didi.etc.hdfs.tool.HDFSUtil
import com.didi.etc.its.common.util.Common
import com.didi.etc.its.common.util.DataFormatUtil
import org.apache.spark.Logging

object OrderGPS extends Logging {
  def main(args: Array[String]): Unit = {

    // 读取配置问件，如果指定则读入指定的配置文件，否则默认conf.xml
    var confFileName = ""
    if (args.size == 1) confFileName = args(0)

    // 提取订单gps业务的属性配置
    val odGPSProp = new OrderGPSProp(confFileName)
    // 提取配置属性
    val odGPSPropBean = odGPSProp.getOrderGPSPProp

    // 打印配置属性列表
    odGPSProp.printPorperties

    val util = new HDFSUtil
    // 获取hdfs实例HDFSFileSytem
    util.HDFSInit
    val startTime = System.currentTimeMillis
    logInfo("订单GPS轨迹数据提取开始运行,开始时间:" + DataFormatUtil.df1.format(startTime))

    //spark app 名字
    val appName = odGPSPropBean.getAppName
    //是否合并partition输出到一个文件中去
    val patUnionFlg = odGPSPropBean.getResutlUnionPathionFlg

    val dirs = odGPSPropBean.getGpsDir.split(",")
    dirs.foreach { dir => util.pathList(new Path(dir)) }
    // 输出结果基本路径
    val confresult = odGPSPropBean.getResultDir
    // 遍历存放（输入）文件的最终路径
    val pareDir = util.pathSet.map { path =>
      {
        val gpsDir = path.toString
        val data_dir = gpsDir.split("/").takeRight(4).mkString("/")
        val result = if (confresult.endsWith("/")) confresult + data_dir else confresult + "/" + data_dir
        // (输入文件夹，输出文件夹)
        (gpsDir, result)
      }
    }
    //---------------删除输出结果依赖的路径---------------
    val hdfs = util.HDFSFileSytem
    pareDir.foreach(path => {
      val outpath = path._2
      val result_path = new Path(outpath)
      if (hdfs.exists(result_path) &&  hdfs.delete(result_path, true)) logInfo("删除orderGPS结果依赖路径："+outpath)
    })
    //-----------------------------------------------
    val sparkConf = new SparkConf
    sparkConf.setAppName(appName)
    val sc = new SparkContext(sparkConf)

    // 从订单数据中以driverid为位单找出订单的产生时间一览表【dirverID,list[(starttime,endtime)]】
    val orderTimeRdd = getOrderList(sc, odGPSPropBean)
    orderTimeRdd.persist

    pareDir.foreach(f => {
      val gpsDir = f._1
      val rsDir = f._2
      //areaGpsRdd格式为：[driverID,(time,gps)]
      val areaGpsRdd = getPGSList(sc, odGPSPropBean, gpsDir)
      //areaGpsRdd:[driverID,(time,gps)], orderTimeRdd:[driverID,(startTime,endTime)]
      val orderRdd = getMatchOrderGPSRdd(areaGpsRdd, orderTimeRdd)
      if (orderRdd.partitions.size > 0) {
        val resultRdd = if (patUnionFlg) orderRdd.coalesce(1) else orderRdd
        resultRdd.saveAsTextFile(rsDir)
      }
    })
    orderTimeRdd.unpersist()

    val endtime = System.currentTimeMillis
    logInfo( "订单GPS轨迹数据提取完毕,结束时间:" + DataFormatUtil.df1.format(endtime))
    val time = (endtime - startTime) / 1000
    logInfo("订单GPS轨迹数据提取任务耗时:" + time + "秒 =" + time / 60 + "分 =" + time / 3600 + "时")
  }

  /**
   *  从订单数据中以driverid为位单找出满足select条件的订单的产生时间一览表【dirverID,list[(starttime,endtime)]】
   */
  def getOrderList(sc: SparkContext, odGPSPropBean: OrderGPSBean) = {
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.sql("use " + odGPSPropBean.getHiveDatabase)

    val itemsArray = odGPSPropBean.getSelect.split(",")
    val sql = "select " + odGPSPropBean.getSelect + " from " + odGPSPropBean.getOrderTable + " where " + odGPSPropBean.getWhere

    val beijingOrder = hiveContext.sql(sql).toJSON.map { jsonStr => Common.jsonStr2valueList(jsonStr, itemsArray) }
    beijingOrder.map(tems => (tems(0), (tems(1), tems(2)))).groupByKey.map { ordertime =>
      {
        val driver_id = ordertime._1
        val timeList = ordertime._2.toList
        (driver_id, timeList)
      }
    }
  }

  /**
   * 从mapmatch中获取指定区域的gps轨迹，（driver_id和gps记录（gpsTime,content））
   */
  def getPGSList(sc: SparkContext, odGPSPropBean: OrderGPSBean, gpsDir: String) = {
    sc.textFile(gpsDir).map(_.split("\t")).filter(_.size == 4).map { items =>
      {
        val driver_id = items(0)
        val gpsList = items(3).split(";")
        val lines = gpsList.map { line =>
          {
            val idx = line.indexOf(",")
            val prefix = line.substring(1, idx)
            var content = ""
            var gpsTime: Option[String] = None
            if (prefix == odGPSPropBean.getAreaID) {
              content = line.substring(idx + 1)
              gpsTime = Some(Common.dateFormat1.format((content.split(",")(0) + "000").toLong).toString)
            }
            (gpsTime, content)
          }
          //除prefix=AreaID的全部过滤掉
        }.filter(p => p._1 != None).map(f => (f._1.get, f._2))
        // 并取driver_id和gps记录（gpsTime,content）
        (driver_id, lines)
      }
    }
  }

  /**
   * 指定区域的gps轨迹与订单数据join根据产生时间找出订单时间内产生的gps数据
   */
  def getMatchOrderGPSRdd(areaGpsRdd: RDD[(String, Array[(String, String)])], orderTimeRdd: RDD[(String, List[(String, String)])]) = {
    areaGpsRdd.join(orderTimeRdd).map(unionLine => {
      val driver_id = unionLine._1
      // Array（GPS产生时间，GPS轨迹数据）
      val areaGps = unionLine._2._1
      // Array（接单时间，终单时间）
      val orderTimes = unionLine._2._2
      val orderGPSList = for {
        gps <- areaGps
        //注过hive查出的订单时间中有["]故在比对大小的时候gps产生时间也应该加入["]订单数据如下：
        // {"driver_id":566397083194898,"begin_charge_time":"2016-12-01 22:14:14","finish_time":"2016-12-01 22:26:54"}
        time = """"""" + gps._1 + """""""
        gpsline = gps._2
        // 判断GPS数据是否是订单GPS数据
        st = for (partime <- orderTimes if (partime._1.compareTo(time) <= 0 && partime._2.compareTo(time) >= 0)) yield "pass"
        if (st.size > 0)
      } yield gpsline
      val count = orderGPSList.size
      if (count > 0) Some(Common.getMD5(driver_id) + "\t" + count + "|" + areaGps.size + "\t" + orderGPSList.mkString(";")) else None
    }).filter(_ != None).map(_.get)
  }
}