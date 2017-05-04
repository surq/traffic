package com.didi.etc.its.tool

/**
 * 运行机器：its_bi@ipd-cloud-collector02 diditool
 */
import org.apache.spark.{ SparkConf, SparkContext, Logging }

object JiNanKaKouJiaoShi extends Logging {
  def main(args: Array[String]): Unit = {
    require(args.size == 2, "请输入HDFS文件路径和输出路径")
    val input = args(0)
    val output = args(1)
    val sparkConf = new SparkConf
    val sc = new SparkContext(sparkConf)
    sparkConf.setAppName("JiNanKaKouJiaoShi")
     sc.textFile(input).map(line => (line.split(";")(4),1)).groupByKey.map { list => list._1}.coalesce(1, true).saveAsTextFile(output)
  }
}