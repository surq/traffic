package com.didi.etc.decode

import java.io.File
import java.security.InvalidKeyException
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom

import scala.io.Source

import javax.crypto.BadPaddingException
import javax.crypto.Cipher
import javax.crypto.IllegalBlockSizeException
import javax.crypto.KeyGenerator
import javax.crypto.NoSuchPaddingException
import javax.crypto.spec.SecretKeySpec
import sun.misc.BASE64Decoder

//object ttt {
//  def main(args: Array[String]): Unit = {
//    //位置信息（可以作为参数）
//    val pospwd = "1,3,16,13,5,7"
//    //    txt2StringForJIE("/Users/didi/work/test/pwd_20161125113411.data", pospwd)
//    var lines = Source.fromFile("/Users/didi/work/test/pwd_20161125113411.data").getLines
//    lines = (new JiNanKakouDecode).decode(lines, pospwd)
//
//    val kv_contens = lines.map(line => {
//      // 行换成字段列表（最终加end字段）
//      val newline = (line + ";" + "end").split(";" )
//      Console println newline(5)
//      // (是否是有效数据flg,key,line)
//      (newline.length == 12 + 1, keySplit(newline(5)), line)
//    }).filter(_._1).map(line => (line._2, line._3)).toList
//    kv_contens.foreach(f=>println(f))
//  }
//
//  
//    def keySplit(key: String) = {
//    val separator = System.getProperty("file.separator")
//    val list = key.split(" ")
//
//    val date = list(0).split("-")
//    val time = list(1).split(":")
//    val result = (date ++ time.take(1))
//    result.mkString(separator, separator, separator)
//  }
//    
//    
//}
class JiNanKakouDecode extends Decode {

  val base64Decoder = new BASE64Decoder
  //  def main(args: Array[String]): Unit = {
  //    //位置信息（可以作为参数）
  //        val pospwd = "1,3,16,13,5,7"
  //    //    txt2StringForJIE("/Users/didi/work/test/pwd_20161125113411.data", pospwd)
  //      var lines = Source.fromFile("/Users/didi/work/test/pwd_20161125113411.data").getLines
  //       lines = decode(lines, pospwd)
  //       lines.foreach { x => println(x) }
  //  }

  /**
   * 解密：
   * @param fileName
   * @throws IOException
   */
  def decode(lines: Iterator[String], pospwd: String, codingFormat: String): Iterator[String] = {
    // 对应下标
    val opsitionList = pospwd.split(",").map { index => index.toInt - 1 }
    lines.map { line =>
      {
        //密码区
        val pwd = line.substring(0, 16)
        //数据信息区
        val info = line.substring(16)
        // AES key
        val aESkey = (for (index <- opsitionList) yield (pwd.substring(index, index + 1))).mkString("")
        val encrypted = base64Decoder.decodeBuffer(info)
        val result = decrypt(encrypted, aESkey);
        new String(result, codingFormat)
      }
    }
  }
  /**
   * 解密AES加密过的字符串
   * @param content
   * AES加密过过的内容
   * @param password
   * 加密时的密码
   * @return 明文
   * @throws UnsupportedEncodingException
   */
  def decrypt(content: Array[Byte], password: String): Array[Byte] = {
    var resutl: Array[Byte] = null
    try {
      // 创建AES的Key生产实例
      val kgen = KeyGenerator.getInstance("AES")
      val random = SecureRandom.getInstance("SHA1PRNG")
      random.setSeed(password.getBytes)
      kgen.init(128, random)
      // 根据用户密码，生成一个密钥
      val secretKey = kgen.generateKey
      // 返回基本编码格式的密钥
      val enCodeFormat = secretKey.getEncoded
      // 转换为AES专用密钥
      val key = new SecretKeySpec(enCodeFormat, "AES")
      // 创建密码器
      val cipher = Cipher.getInstance("AES")
      // 初始化为解密模式的密码器
      cipher.init(Cipher.DECRYPT_MODE, key)
      resutl = cipher.doFinal(content)
    } catch {
      case e1: NoSuchAlgorithmException  => e1.printStackTrace
      case e2: NoSuchPaddingException    => e2.printStackTrace
      case e3: InvalidKeyException       => e3.printStackTrace
      case e4: IllegalBlockSizeException => e4.printStackTrace
      case e5: BadPaddingException       => e5.printStackTrace
      case e6: Exception                 => e6.printStackTrace
    }
    resutl
  }
}