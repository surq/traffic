package com.didi.etc.its.common.util

object Common {

  // 时间格式
  val dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmm")
  val dateFormat1 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dateFormat2 = new java.text.SimpleDateFormat("\\yyyy\\MM\\dd\\")

  //--------------------------------Json ----------------------------------------------
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
  //-----------------------MD5---------------------------
  import java.math.BigInteger;
  import java.security.MessageDigest;
  import java.security.NoSuchAlgorithmException;
  def getMD5(value: String) = new BigInteger(1, MessageDigest.getInstance("MD5").digest(value.getBytes())).toString(16)
  //--------------------------------------------------
}