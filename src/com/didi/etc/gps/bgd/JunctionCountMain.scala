package com.didi.etc.gps.bgd

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext

/**
 * @author 宿荣全
 * 指定路口经纬度统计过车流量
 */
object JunctionCountMain {

  def main(args: Array[String]): Unit = {

    // 解析加载用户属性配置
    val propBean = Util.analysis
    // 统计路口过车周期（比如：5分钟）
    val duration = propBean.getDuration
    // app在yarn中的名字
    val appName = propBean.getAppName
    // mapMatch的gps轨迹数据存放路径
    val mmDir = propBean.getMapmatchDir
    // 指定的路口经纬度列表
    val junctionList = propBean.getJunctionPoints
    // 经纬度覆盖范围
    val area = propBean.getArea
    // 统计车流量结果存放路径
    val countCarDir = propBean.getCountCarDir
    // 统计车流量结果存放路径
    val countUserDir = propBean.getCountUserDir
    val sparkConf = new SparkConf
    sparkConf.setAppName(appName)

    val sc = new SparkContext(sparkConf)
    //----------------------------------------------------------------

    val junctionRdd = sc.textFile(mmDir).map { line =>
      {
        // user_id、用户电话号码、地图版本、该小时内的轨迹结果
        val items = line.split("\t")
        //该小时内的gps轨迹结果以";"分隔：时间（秒）、轨迹点经度、轨迹点纬度	点匹配的linkID（可以为多条）、
        // 距link始点的距离、投影点的经度、投影点的维度、点速度、点方向、匹配结果置信度
        val gpsList = items(3).split(";")
        val gpsRecords = gpsList.map { record =>
          {
            val itemList = record.split(",")
            //itemList(5),itemList(6)映射到地图上的经、纬度
            val lat = itemList(5).toFloat
            val lng = itemList(6).toFloat

            // 此条记录是否通过路口列表中的一个路口，若通过则返回路口index，否则为None.
            Util.getJunction(junctionList, (lat, lng), area) match {
              case Some(junctionIndex) => {
                // 以duration划分时间分段
                val timeSlot = Util.dateFormat.format(itemList(0).toLong * 1000).toLong / duration * duration
                // (路口index，时间段,userid) 一个路口一个时间分片一个司机多次经过的情况下算做一次流量
                val key = junctionIndex + "_" + timeSlot + "_" + items(0)
                Some(key)
              }
              case None => None
            }
          }
        }
        // 统计此用户本条gps记录的轨迹都经过了哪些路口(去重)，只保存过路口的gps数据
        gpsRecords.filter(record => {
          record match {
            case Some(e) => true
            case None    => false
          }
          // 一条轨迹内的时间分片内的一个用户经过某一路口的多条记录合并为一条
        }).groupBy(f => f.get).map(f => f._1)
      }
    }.flatMap(f => f).persist
    //------------------------------某一路口某一时间段的车流量------------------------------------------------
    //(路口index，时间段分钟,userid) 
    val result1 = junctionRdd.map(key => key.split("_").take(2).mkString(",")).groupBy(f=>f).map(key => {
        //(路口index，时间段) 
      val index_time = key._1.split(",")
      val junctionPoints = junctionList(index_time(0).toInt)._1
      key._1 + "," + junctionPoints + "," + key._2.size
    }).sortBy(key => key, true, 1)
    if (result1.partitions.size > 0) result1.saveAsTextFile(countCarDir)

    //------------------------------某一路口某一时间段经过的司机-----------------------------------------------
    //(路口index_时间段分钟_userid)
    val result2 = junctionRdd.map { record =>
      {
        //(路口index，时间段(例如：201612140955),userid) 
        val items = record.split("_")
        val hour = items(1).substring(0, 10)
        items(0) + "_" + hour + "_" + items(2)
      }
      // 同一小时同一司机经地此路口合并为一条记录
    }.groupBy(f => f).map(key => key._1).groupBy(f =>f.split("_").take(2).mkString(",")).map(f=>{
      val index_hour = f._1.split(",")
      val junctionPoints = junctionList(index_hour(0).toInt)._1
      f._1 +","+junctionPoints+","+f._2.size
    }).sortBy(key => key, true, 1)
    
    if (result2.partitions.size > 0) result2.saveAsTextFile(countUserDir)

    junctionRdd.unpersist()
  }
}