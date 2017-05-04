package com.didi.etc.its.tool

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf,Logging}
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TMemoryBuffer

import com.didi.util.auto.GPS
import com.didi.etc.hdfs.tool.HDFSUtil
import com.didi.etc.prop.PolygonStreamingProp
import com.didi.etc.its.common.util.DataFormatUtil
import com.didi.etc.prop.bean.{PolygonUserConfBean,LngLatBean}
import com.didi.etc.its.common.util.{Common,KafkaProducer}

import kafka.serializer.DefaultDecoder
import didi.traffic.cd.receiver.positionprotocol.thrift.{MapMatchRes2SpdCalcRequest,map_match_point_t}
import didi.traffic.cd.receiver.positionprotocol.thrift.MapMatcherService.Processor

object PolygonGPSStreaming extends Logging {

  def main(args: Array[String]): Unit = {

    require(args.size > 0, "请输入PolygonGPSStreaming 的参数配置文件名字，例如：polygonStreamConf.xml")
    logWarning("PolygonGPSStreaming的配置文件为：" + args(0))

    // 读取spark配置文件(conf路径下)
    val sparkConfbean = getStreamingConf(args(0))
    logWarning("用户配置文件：" + sparkConfbean.getUserConfDir)

    // 获取用户配置文件
    val userConfList = getUserConf(sparkConfbean.getUserConfDir)
    require(userConfList.size > 0, "polygonUserConf.conf 文件中的配置内容不对或为空，请check详细内容！")

    // 多边形列表和区域ID
    val polygonList = userConfList.map { userConfBean =>
      {
        val pairPoints = new LngLatBean
        pairPoints.setLng_latList(userConfBean.getPlygonArray)
        pairPoints.setPolygonID(userConfBean.getPolygonID)
        pairPoints
      }
    }
    //打印区域以及座标信息
    polygonList.foreach(bean => logWarning("区域ID：" + bean.getPolygonID + "座标：" +
      (for (points <- bean.getLng_latList) yield (points(0) + "," + points(1))).mkString(":")))

    val sparkConf = new SparkConf().setAppName(sparkConfbean.getAppName)
    val ssc = new StreamingContext(sparkConf, Seconds(sparkConfbean.getInterval))

    // kafka producer的BrokerList
    val brokerList = sparkConfbean.getBrokerList
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList,
      "group.id" -> sparkConfbean.getGroup, "zookeeper.connect" -> sparkConfbean.getZookeeper)


    var inputDstream: DStream[Array[Byte]] = null
    if (sparkConfbean.getDirectAPI) {
      inputDstream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
        ssc, kafkaParams, sparkConfbean.getTopic.split(",").toSet).map(_._2)
    } else {
      val kafkaStreams = (0 until sparkConfbean.getReceverNum).map(num => {
        KafkaUtils.createStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
            ssc, kafkaParams, Map(sparkConfbean.getTopic -> 1), StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
      })
      inputDstream = ssc.union(kafkaStreams)
    }

    val request = new MapMatchRes2SpdCalcRequest
    // map:解码成行（实列对像），filter：只取用户配置的区域 gulf gps
    val fileDstream = inputDstream.map { dataByte =>
      {
        val tmb = new TMemoryBuffer(dataByte.length)
        tmb.write(dataByte)
        val oprot = new TBinaryProtocol(tmb)
        request.read(oprot)
        val list = request.getMap_match_point_vec.asScala
        (for (bean <- list) yield (getPolygon(bean, polygonList))).toList.filter(_ != None)
      }
    }.flatMap(f => f).map(_.get)

    fileDstream.foreachRDD(rdd => {
      if (rdd.partitions.size > 0) {
        rdd.persist
        userConfList.foreach { polygonInfo =>
          {
            val polygonID = polygonInfo.getPolygonID
            val outPutdir = polygonInfo.getOutPutDir
            val runTime = DataFormatUtil.df4.format(System.currentTimeMillis)
            val outPut = outPutdir + "/" + runTime
            val OutPutTopic = polygonInfo.getOutKafkaTopic
            logWarning(">>>>>>>>>> 正在处理 polygonID：" + polygonID)
            logWarning(">>>>>>>>>> 结果输出 路径：" + outPut)
            // 过滤其它区域，输出value
            val resultRDD = rdd.filter(_._1 == polygonID).map(line => (line._2.getUser_id, line._2)).groupBy(_._1).map(valueList => {
              val lineList = valueList._2.toList.map(value => value._2)
              val bean = lineList(0)
              val headIfo = Common.getMD5(bean.getUser_id) + "\t" + lineList.size + "|" + lineList.size + "\t"
              val gpsList = lineList.map { bean =>
                {
                  bean.getTimestamp + "," +
                    pointFormat(bean.getSrc_x) + "," +
                    pointFormat(bean.getSrc_y) + "," +
                    bean.getLink_id_vec.asScala.mkString("|") + "," +
                    bean.getLink_pass_dist + "," +
                    pointFormat(bean.getProj_x) + "," +
                    pointFormat(bean.getProj_y) + "," +
                    DataFormatUtil.d2.format(bean.getLine_speed) + "," +
                    DataFormatUtil.d2.format(bean.getLine_direction) + "," +
                    bean.getCertainty
                }
              }
              headIfo + gpsList.mkString(";")
            }).coalesce(1).mapPartitions(KafkaProducer.sparkSendKafka(_,brokerList,OutPutTopic), true).count()
            //TODO
//            // 数据写两份
//            resultRDD.persist
//            // 写kafka
//            resultRDD.mapPartitions(KafkaProducer.sparkSendKafka(_,brokerList,OutPutTopic), true).count()
//            // 写文件
//            resultRDD.saveAsTextFile(outPut)
//            resultRDD.unpersist()
          }
        }
        rdd.unpersist()
      }
    })
    ssc.start
    ssc.awaitTermination
  }

  /**
   * 座标格式化
   */
  def pointFormat(value: Int) = {
    val strValue = value.toString
    val restr = strValue.splitAt(strValue.length - 5)
    restr._1 + "." + restr._2
  }
  /**
   * 只抽取指定区域内的，专快gps轨迹
   */
  def getPolygon(bean: map_match_point_t, polygonList: List[LngLatBean]) = {
    // gulf:258,taxi:257
    if (bean.getBiztype != 258) None else {
      val lng_format = pointFormat(bean.getSrc_x)
      val lat_format = pointFormat(bean.getSrc_y)
      val idList = for (polygon <- polygonList if (GPS.inPolygon(lng_format.toDouble, lat_format.toDouble, polygon.getLng_latList))) yield polygon.getPolygonID
      if (idList.size > 0) Some((idList(0), bean)) else None
    }
  }

  /**
   * 读取spark配置文件信息
   */
  def getStreamingConf(confPath: String) = new PolygonStreamingProp(confPath).getProp

  /**
   * 读取HDFS自动化系统配置
   */
  def getUserConf(confPath: String) = {
    val hdfsutil = new HDFSUtil
    hdfsutil.HDFSInit
    var HDFSFileSytem = hdfsutil.HDFSFileSytem
    val path = new Path(confPath)

    if (HDFSFileSytem.exists(path)) {
      val srcDFSInput = HDFSFileSytem.open(path)
      //过滤掉配置文件中以"#"开头的注释文件
      val line_content = hdfsutil.getHdfsFileLines(srcDFSInput).map(_.trim).filter(line => (!line.startsWith("#") && line.trim != ""))
      val properiesList = line_content.tail.map(_.split(","))

      properiesList.map { lineArray =>
        {
          val bean = new PolygonUserConfBean
          // 多边形所在城市,任务名称,区域ID,区域座标，输出根目录,输出kafka-topic
          val city = lineArray(0)
          val taskName = lineArray(1)
          val polygonID = lineArray(2)
          val points = lineArray(3)
          val outPath = lineArray(4) + "/" + city + "/" + taskName
          val outKafkaTopic = lineArray(5)
          logWarning("PolygonInfo：city=" + city + " taskName=" + taskName + " polygonID=" + polygonID + " points=" + points + " outPath=" + outPath + " kafkaOutTopic:" + outKafkaTopic)
          // 解析成座标对
          val lng_latList = points.split(";").map { points =>
            {
              val par = points.split("@")
              // 座标精度只保留4位
              Array(DataFormatUtil.d1.format(par(0).toDouble).toDouble, DataFormatUtil.d1.format(par(1).toDouble).toDouble)
            }
          }
          bean.setPolygonCity(city)
          bean.setPolygonTaskName(taskName)
          bean.setPolygonID(polygonID)
          bean.setPlygonArray(lng_latList)
          bean.setOutPutDir(outPath)
          bean.setOutKafkaTopic(outKafkaTopic)
          bean
        }
      }
    } else Nil
  }
}