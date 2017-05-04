package com.didi.etc.its.tool

import org.apache.spark.{ SparkConf, Logging }
import org.apache.spark.SparkContext
import com.didi.etc.prop.JiNankakouLuKouCountProp
import com.didi.etc.its.common.util.DataFormatUtil
import java.util.Calendar
import java.sql.DriverManager
import com.didi.etc.prop.bean.JiNankakouLuKouCountBean
import java.sql.Connection

case class KakouInfo(kakouId: String, time: String, direction: String)

object JiNankakouLuKouCount extends Logging {
  def main(args: Array[String]): Unit = {

    require(args.size > 0, "请输入JiNankakouLuKouCount 的参数配置文件名字，例如：JiNankakouLuKouCountConf.xml")
    logWarning("JiNankakouLuKouCount的配置文件为：" + args(0))

    val confBean = new JiNankakouLuKouCountProp(args(0)).getProp
    val sparkConf = new SparkConf
    val sc = new SparkContext(sparkConf)
    sparkConf.setAppName(confBean.getAppName)

    // 车辆md5,品牌,车身颜色,车牌颜色,卡口编号,通过时间,瞬时速度,车道编号,行车方向,车辆状态,通过地点,抓拍方向
    val kakouItemList = confBean.getKakouItemList
    val kakouSize = kakouItemList.size
    val cityid = confBean.getCityId
    val countInterval = confBean.getCountInterval * 60 * 1000
    val kakouDescribeList = confBean.getKakouDescribeList
    val kakouList = confBean.getKakouIdList

    val id_nameArray = kakouList.zip(kakouDescribeList)

    //(kakouID,name)
    val id_naemList = id_nameArray.map(record => {

      val idList = record._1.split(",")
      val name = record._2
      idList.map { id => (id, name) }
    }).flatMap(f => f).toList

    val kakouMap = id_naemList.toMap

    // 卡口ID列表
    val kakouIdList = for (kakouInfo <- id_naemList) yield kakouInfo._1

    val batchPattern = confBean.getBatchPattern
    var sourcePath = ""

    if (batchPattern == "batch") {
      sourcePath = confBean.getKakouPath
    } else if (batchPattern == "day") {
      val date = args(1)
      val year = date.substring(0, 4)
      val mon = date.substring(4, 6)
      val day = date.substring(6, 8)
      sourcePath = confBean.getKakouPath + "/" + year + "/" + mon + "/" + day+ "/*"
    }

    sc.textFile(sourcePath).map(line => {
      val lineList = line.split(";")
      val kakouId = lineList(4)
      val timeTmp = lineList(5)
      val direction = lineList(8)
      val info = KakouInfo(kakouId, timeTmp, direction)
      // 过滤掉字段个数不一致的、过滤掉卡口ID不是指定列表的ID
      if (lineList.size != kakouSize) None else if (kakouIdList.contains(kakouId)) Some(info) else None
    }).filter(_ != None).map(opt => {
      val kakouinfo = opt.get
      // 2017-02-28 08:16:17 270
      val date = kakouinfo.time
      val kakouId = kakouinfo.kakouId

      val calendar = Calendar.getInstance
      // 指定期数据
      val yearTmp = date.substring(0, 4)
      val monthTmp = date.substring(5, 7)
      val dayTmp = date.substring(8, 10)
      val hour = date.substring(11, 13)
      val minute = date.substring(14, 16)

      calendar.set(Calendar.YEAR, yearTmp.toInt)
      calendar.set(Calendar.MONTH, monthTmp.toInt - 1)
      calendar.set(Calendar.DAY_OF_MONTH, dayTmp.toInt)
      calendar.set(Calendar.HOUR_OF_DAY, hour.toInt)
      calendar.set(Calendar.MINUTE, minute.toInt)
      val subTime = calendar.getTimeInMillis / countInterval * countInterval
      val newTime = calendar.setTimeInMillis(subTime)
      val keyTime = DataFormatUtil.df5.format(calendar.getTime)

      val junctionname = kakouMap(kakouId)
      (cityid + "," +junctionname + "," + kakouId + "," + keyTime + "," + kakouinfo.direction, 1)
    }).groupBy(_._1).coalesce(1).mapPartitions(its => {
      val list = its.toList
      if (list.size > 0) {
        val valueList = list.map(record => (record._1 + "," + record._2.toList.size).split(","))
        putSqlData(confBean, valueList)
      }
      list.toIterator
    }, true).count
  }

  /**
   * 结果存入数据库
   */
  def putSqlData(confbean: JiNankakouLuKouCountBean, valueList: List[Array[String]]) = {
    val driver = "com.mysql.jdbc.Driver"
    val url = confbean.getOut_url
    val username = confbean.getOut_username
    val password = confbean.getOut_password
    val tableName = confbean.getOut_table
    var connection: Connection = null
    if (valueList.size > 0) {
      try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement()
        valueList.foreach { values =>
          {
            val sqlValue = values.mkString("'", "','", "'")
            val sql = "insert into " + tableName + " (city_id, junctionname, kakou_id, date, direction, flow_count) values (" + sqlValue + ");"
            statement.executeUpdate(sql);
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