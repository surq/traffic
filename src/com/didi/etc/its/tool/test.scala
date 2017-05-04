package com.didi.etc.its.tool

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SQLContext, DataFrame, Row }
import breeze.linalg.max

object test {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("DateFrameTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val df = jsonToDF(sqlContext)
    rddToDF(sc, sqlContext)
//    rdd_schame_to_DF(sc, sqlContext)

  }

  def jsonToDF(sqlContext: SQLContext) = {
    val df = sqlContext.read.json("/Users/didi/work/t.txt")
    df.show()
    //+----+---+-----+
    //| 人口|城市|  面积|
    //+----+---+-----+
    //|1600| 北京|16800|
    //|1800| 上海| 6400|
    //+----+---+-----+
    df.printSchema()
    //root
    // |-- 人口: long (nullable = true)
    // |-- 城市: string (nullable = true)
    // |-- 面积: long (nullable = true)
    df.select("城市").show()
    //+---+
    //| 城市|
    //+---+
    //| 北京|
    //| 上海|
    //+---+
    df.select(df("城市"), df("面积") + 1).show()
    //+---+--------+
    //| 城市|(面积 + 1)|
    //+---+--------+
    //| 北京|   16801|
    //| 上海|    6401|
    //+---+--------+
    df.filter(df("面积") > 6400).show()
    //+----+---+-----+
    //|  人口| 城市|   面积|
    //+----+---+-----+
    //|1600| 北京|16800|
    //+----+---+-----+
    df.groupBy("面积").count().show()
    //+-----+-----+
    //|   面积|count|
    //+-----+-----+
    //|16800|    1|
    //| 6400|    1|
    //+-----+-----+
    df.registerTempTable("surq")
    val df_sql = sqlContext.sql("SELECT * FROM surq")
    df_sql.show()
    //+----+---+-----+
    //|  人口| 城市|   面积|
    //+----+---+-----+
    //|1600| 北京|16800|
    //|1800| 上海| 6400|
    //+----+---+-----+
    // 通过index或列名获取值
    df_sql.map(row => "城市: " + row(1)).collect().foreach(println)
    // or by field name:
     df_sql.map(t => "城市: " + t.getAs[String]("城市")).collect().foreach(println)
    //城市: 北京
    //城市: 上海
     df_sql.map(_.getValuesMap[Any](List("城市", "人口", "面积"))).collect().foreach(println)
    //Map(城市 -> 北京, 人口 -> 1600, 面积 -> 16800)
    //Map(城市 -> 上海, 人口 -> 1800, 面积 -> 6400)
    df
  }

  def rddToDF(sc: SparkContext, sqlContext: SQLContext) = {

    import org.apache.spark.sql.types.{ StringType, StructField, StructType }

    //-----定义schema把rdd转换成dataFrame start-----------
    val rdd = sc.textFile("/Users/didi/work/t1.txt")
    val schemaString = "城市,面积,人口"
    // 根据schema创建StructField(此处型类全部是StringType)
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    // 把数据转换成RDD【Row】
    val rowRDD = rdd.map(_.split(",")).map(p => Row(p(0), p(1), p(2)))
    // RDD[Row]转换成DataFrame
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //注册成临时表
    peopleDataFrame.registerTempTable("city")
    // 用sqlContext对象提供的sql方法执行SQL语句。
    val results = sqlContext.sql("SELECT 城市,面积,人口 FROM city")
    results.map(t => "城市: " + t(0) + " 面积: " + t(1) + " 人口: " + t(2)).collect().foreach(println)
    //城市: 北京 面积: 16800 人口: 1600
    //城市: 上海 面积: 6400 人口: 1800

    //-----定义schema把rdd转换成dataFrame start-----------

    // Schema自动合并功能(hive分区的方式)
    import sqlContext.implicits._
    // 创建DataFrame
    val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.mode("append").parquet("/Users/didi/work/tab/key=1")

    //创建另一个DataFrame
    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.mode("append").parquet("/Users/didi/work/tab/key=2")

    // Read the partitioned table必需设mergeSchema = true
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("/Users/didi/work/tab")
    df3.printSchema()
    df3.show()
    //+------+------+------+---+
    //|single|double|triple|key|
    //+------+------+------+---+
    //|     1|     2|  null|  1|
    //|     2|     4|  null|  1|
    //|     3|     6|  null|  1|
    //|     4|     8|  null|  1|
    //|     5|    10|  null|  1|
    //|     6|  null|    18|  2|
    //|     7|  null|    21|  2|
    //|     8|  null|    24|  2|
    //|     9|  null|    27|  2|
    //|    10|  null|    30|  2|
    //+------+------+------+---+
    //Hive Tables
    //val hive_sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //hive_sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    //val ssk = hive_sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")
    //hive_sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)
    //ssk.write.mode("orc").saveAsTable("table")
    
//    results.groupBy("城市").agg(count($"面积"),max($"面积").as("maxAge"), avg($"age").as("avgAge")).show
//    城市,面积,人口
    results.groupBy("城市").agg("城市"->"count","面积" -> "max", "人口" -> "avg").show
  }

  /**
   * 定义多类型的数据结构
   */
  def rdd_schame_to_DF(sc: SparkContext, sqlContext: SQLContext) = {

    object schemas {
      import org.apache.spark.sql.types.{ StructField, StringType, LongType, StructType }
      // 北京,16800,1600
      val name = StructField("name", StringType, true)
      val area = StructField("area", LongType, true)
      val number = StructField("number", LongType, true)
      val row = StructType(Array(name, area, number))
    }
    val rdd = sc.parallelize(Array(("scala", 100, 1000), ("java", 100, 1000), ("spark", 100, 1000)), 1)
    val rowRdd = rdd.map(x => Row(x._1, x._2, x._3)) //转换为Row  
    val df4 = sqlContext.createDataFrame(rowRdd, schemas.row) //创建dataframe  
    df4.select("name", "area", "number").show()
  }

  def dfOption(df: DataFrame, sqlContext: SQLContext) = {
    //一些常见的SQL查询用法
    //http://blog.csdn.net/dreamer2020/article/details/51284789

    // text只支持一列输出，json可以多列。
    //format: [json|parquet|orc(hiveContext)] mode:['overwrite', 'append', 'ignore', 'error']
    //saveAsTable
    df.select("城市", "面积").write.format("json").mode("append").save("/Users/didi/work/ttt")
    df.select("城市", "面积").write.mode("append").parquet("/Users/didi/work/people.parquet")
    val parquetFile = sqlContext.read.parquet("/Users/didi/work/people.parquet")
    parquetFile.registerTempTable("parquetFile")
    val res = sqlContext.sql("SELECT * FROM parquetFile ")
    res.map(t => "CityName: " + t(0)).collect().foreach(println)

    val dfs = sqlContext.sql("SELECT * FROM parquet.`/Users/didi/work/people.parquet`")
    dfs.show()
  }
}



