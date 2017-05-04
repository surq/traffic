package com.didi.etc.its.tool

import java.sql.Connection
import java.sql.DriverManager

import scala.collection.mutable.ArrayBuffer

import scala.collection.JavaConverters._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import com.didi.etc.its.common.util.DataFormatUtil
import com.didi.etc.its.common.util.Common
import scala.collection.mutable.HashMap
import org.apache.spark.storage.StorageLevel
import kafka.serializer.StringDecoder
import com.didi.etc.its.flowduration.FlowUtil
import scala.io.Source
import com.didi.etc.prop.JunctionStreamingProp
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import com.didi.etc.prop.bean.JunctionStreamingBean

/**
 * Created by yangfei on 17/3/6.
 *   Example:
 */
object Calculate {

  def main(args: Array[String]) {

    val confbean = (new JunctionStreamingProp(args(0))).getProp
    val zkQuorum = confbean.getZookeeper
    //    val outPath = "/user/its_bi/yangfei/test/calculate"
    val Interval = confbean.getInterval
    val durationValue = confbean.getDurationValue
    val flowConfPath = confbean.getFlowConfPath
    val junctionList = confbean.getJunctionList
    val sqlStrOrderTable = confbean.getIn_table
    val topic = confbean.getTopic
    // --------------兵兵 start-----------------------
    val HDFSFileSytem = FileSystem.get(new Configuration)
    val srcDFSInput = HDFSFileSytem.open(new Path(flowConfPath))
    val flowMap = FlowUtil.getFlowFromFile(srcDFSInput)
    // --------------兵兵 end-----------------------

    val sparkConf = new SparkConf().setAppName("OrderGPSStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(Interval))

    val topicMap = Map("jinan_order_gps_task1" -> 1)
    val kafkaParams = Map[String, String]("group.id" -> "its", "zookeeper.connect" -> zkQuorum)

    val kafkaStreams = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)

    //合并分区,重新分组,排序,筛选
    val dataRdd = kafkaStreams.map(lines => {
      val line = lines.split("\t")
      ((line(0), line(2)))
    }).groupByKey.map(lines => {
      val gpsLists = lines._2.toList.map(line => line.split(";")).flatMap(line => line)
      // 时间按升序排序
      val list = gpsLists.map(line => (line.substring(0, line.indexOf(",")), line)).sortWith(_._1 < _._1).map(record => record._2)
      val minTime = list(0).substring(0, list(0).indexOf(","))
      val lastRecord = list(list.size - 1)
      val maxTime = lastRecord.substring(0, lastRecord.indexOf(","))
      // driverID,最大时间，最小时间,gpsList
      (lines._1, maxTime, minTime, list)
    }).foreachRDD(rdd => {
      if (rdd.partitions.size > 0) {
        rdd.coalesce(1).mapPartitions(its => {
          val list = its.toList
          var resultMap_duration_distribute = scala.collection.mutable.Map[String, Tuple2[Int, Int]]()
          val resultList = if (list.size > 0) {
            val maxList = ArrayBuffer[String]()
            val minList = ArrayBuffer[String]()
            val driverIDList = ArrayBuffer[String]()
            for (recode <- list) { driverIDList += recode._1; maxList += recode._2; minList += recode._3 }
            // 降序
            val maxTime = maxList.sortWith(_ > _)(0)
            val minTime = minList.sortWith(_ < _)(0)

            // 加载静态表flow_duration_distribute
            resultMap_duration_distribute = getflow_duration_distributeData
            val sqlData = getSqlData(confbean, driverIDList.toList, DataFormatUtil.df1.format(maxTime.toLong * 1000), DataFormatUtil.df1.format(minTime.toLong * 1000), DataFormatUtil.df1.format(minTime.toLong * 1000 - 1000 * 60 * 60))
            val sqlMap = sqlData.map(reslt => (reslt._1, reslt)).groupBy(_._1).map(record => (record._1, record._2.map(f => (f._2._2, f._2._3))))
            val orderGpsList = list.map(list => {
              val driverId = list._1
              val gpsList = list._4
              // gps总条数
              val GpsCount = gpsList.size
              val start_endtimeList = sqlMap.getOrElse(driverId, Nil)
              // 司机订单gps轨迹
              val userOrderGpsList = if (start_endtimeList != Nil) {
                gpsList.map { gps =>
                  {
                    val timeStr = gps.substring(0, gps.indexOf(","))
                    val time = DataFormatUtil.df1.format(timeStr.toLong * 1000)
                    val sumCount = for (star_end <- start_endtimeList if (time >= star_end._1 && time <= star_end._2)) yield 1
                    if (sumCount.sum > 0) Some(gps) else None
                  }
                }.filter(_ != None).map(_.get)
              } else Nil

              if (userOrderGpsList != Nil && userOrderGpsList.size != 0) {
                val returnStr = driverId + "\t" + userOrderGpsList.size + "|" + GpsCount + "\t" + userOrderGpsList.mkString(";")
                Some(returnStr)
              } else None

            }).filter(_ != None).map(_.get)
            orderGpsList
          } else List[String]()

          // ------------兵兵侧指标计算start--------------------
          // 先对gps数据按时间升序，然后计算指标
          val flowResutList = resultList.map(line => (line.split(",")(0), line)).sortBy(f => f._1).map(_._2).map { line =>
            {
              val durationList = FlowUtil.mapFuction(junctionList, durationValue, line, flowMap).asScala
              durationList.map { line =>
                {
                  val kv = line.split("\t")
                  val time = kv(2).split(":")
                  val areaTime = time(0) + ":" + (if (time(1).toInt < 29) "00" else "30")
                  // junctionID,时间（08：00／08：30），方向，旅行时间
                  // (kv(1),areaTime,kv(5),kv(7))
                  val values = resultMap_duration_distribute.getOrElse(kv(1) + "#" + areaTime + "#" + kv(5), null)
                  val exception_flag = if (values != null && (kv(7).toInt > values._2 || kv(7).toInt < values._1)) 0 else 1
                  line + "\t" + exception_flag
                }
              }
            }
          }.flatMap(f => f)
          // 更新数据库
          putSqlData(confbean, flowResutList)
          // ------------兵兵侧指标计算 end---------------------
          flowResutList.toIterator
        }, false).count()
        //        .saveAsTextFile(outPath + "/" + DataFormatUtil.df4.format(System.currentTimeMillis))
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 取flow_duration_distribute中的静态数据
   * // junctionID,时间（08：00／08：30），方向
   */
  def getflow_duration_distributeData = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://100.90.164.31:3306/its"
    val username = "flow"
    val password = "Znjty@Didi@2017"
    val table = "flow_duration_distribute"

    var connection: Connection = null
    val resultMap = scala.collection.mutable.Map[String, Tuple2[Int, Int]]()
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val sql = "select junctionid,hour,direction,min, max from " + table + ";"
      val resultSet = statement.executeQuery(sql)

      while (resultSet.next()) {
        val key = resultSet.getString("junctionid") + "#" + resultSet.getString("hour").toString + "#" + resultSet.getString("direction")
        resultMap += (key -> (resultSet.getInt("min"), resultSet.getInt("max")))
      }
      statement.close()
    } catch {
      case e: Exception => e.printStackTrace
    } finally {
      connection.close
    }
    resultMap
  }

  /**
   * 取mysq中的数据
   */
  def getSqlData(confbean: JunctionStreamingBean, driver_idList: List[String], maxTime: String, minTime: String, minTimePre: String) = {
    val driver = "com.mysql.jdbc.Driver"
    val url = confbean.getUrl
    val username = confbean.getUsername
    val password = confbean.getPassword
    val sqlStrOrderTable = confbean.getIn_table
    var connection: Connection = null
    val resultList = ArrayBuffer[Tuple3[String, String, String]]()
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select driver_id, begin_charge_time, case finish_time when '0000-00-00 00:00:00' then '9999-01-01 00:00:00' else finish_time end as finish_time FROM " +
        sqlStrOrderTable + " where begin_charge_time != '0000-00-00 00:00:00' and ( finish_time >= '" + minTime + "' or order_status=4);")
      var begin_charge_time = ""
      while (resultSet.next()) resultList += ((Common.getMD5(resultSet.getString("driver_id")), resultSet.getTimestamp("begin_charge_time").toString, resultSet.getTimestamp("finish_time").toString))
      statement.close()
    } catch {
      case e: Exception => e.printStackTrace
    } finally {
      connection.close
    }
    resultList.toList
  }

  /**
   * 结果存入数据库
   */
  def putSqlData(confbean: JunctionStreamingBean, valueList: List[String]) = {
    val driver = "com.mysql.jdbc.Driver"
    val url = confbean.getOut_url
    val username = confbean.getOut_username
    val password = confbean.getOut_password
    val sqlStrOrderTable = confbean.getOut_table
    var connection: Connection = null
    if (valueList.size > 0) {
      try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement()
        valueList.foreach { line =>
          {
            val values = line.split("\t")
            val value = "'" + values(0) + "', " + values(1) + ", '" + values(2) + "', " + values(3) + ", " + values(4) + ", " + values(5) + ", " + values(6) + ", 1, " + values(7) + ", " + values(8)
            statement.executeUpdate("insert into " + sqlStrOrderTable + " (date, junctionid, hour, linkid1, linkid2, direction, turn, count, sum,exception) values (" + value + ");");
          }
        }
        statement.close()
      } catch {
        case e: Exception => e.printStackTrace
      } finally {
        connection.close
      }
    }
  }
}