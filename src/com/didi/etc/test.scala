package com.didi.etc

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.json4s.JsonAST
import org.json4s.JsonAST.JString
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL.pair2Assoc
import org.json4s.JsonDSL.string2jvalue
import org.json4s.jackson.JsonMethods.compact
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.JsonMethods.render
import org.json4s.jvalue2monadic
import org.json4s.string2JsonInput
import com.didi.etc.its.common.util.DataFormatUtil
import java.util.Calendar
import com.didi.etc.decode.JiNanKakouDecode
import scala.io.Source

object tests extends Logging {

  def main(args: Array[String]): Unit = {
    //    require(false, "sfsf")
    //    assert(false, "sfsf")
    logError("logError--------------")
    logInfo("info--济南解密---------")
        val aeskey = "1,3,16,13,5,7"
        val lines = Source.fromFile("/Users/didi/Documents/liuliang04051440.flowdata").getLines().filter(_.trim()!="")
        val jncode = new JiNanKakouDecode
        val valuelines = jncode.decode(lines, aeskey, "UTF-8").toList
        writeFile("/Users/didi/work/test/liuliang04051440.flowdata",valuelines.mkString("\n"))
        valuelines foreach println
    logWarning("logWarning----------")

  }
  
  
    /**
   * 写本地文件
   */
  def writeFile(outputPath: String, str: String) = {
    import java.io._
    val writer = new PrintWriter(new File(outputPath), "UTF-8")
    writer.write(str)
    writer.close
  }
  
  

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

  def jsonStr2JValue(jsonStr: String): JValue = parse(jsonStr)
  def jsonStr2valueList(jsonStr: String, fields: Array[String]): Array[String] = for (field <- fields) yield compact(parse(jsonStr) \ field)

  def isMatch(fileName: String, filterRegex: String) = fileName.matches(filterRegex)
  /**
   * 按规则判断是否是属于要删除的文件
   */
  def isDeleteFile(fileName: String, filterRegex: String, del_flg: Boolean) = {
    if (del_flg) {
      if (isMatch(fileName, filterRegex)) true else false
    } else {
      if (isMatch(fileName, filterRegex)) false else true
    }
  }
}