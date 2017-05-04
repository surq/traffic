package com.didi.util.auto
import scala.xml.XML
import java.io.File
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

object CreateDataMain {

  def main(args: Array[String]): Unit = {
//    getPsconfInfo
    val tempList = List("a", "ab", "abc", "abcd", "abcde", "abcdef", "abcdefg", "abcdefgh")

    val numpattern = "[0-9]+".r
    // 逐条处理匹配的数据
val wordrege = "([0-9]+) ([a-z]+)".r
    for (matchString <- wordrege.findAllIn("99 bottles, 98 bottles")) println("找出所有复合条件的：" + matchString)
  
    
     val list = List(List("a", "b", "c"), List("aa", "bb", "cc"))
     
    val res = list.flatMap(list => { list.map(f => { ("flatMap", f) }) })
    res foreach println
    
  }
  


  
  /**
   * 提取配置文件信息
   */
  def getPsconfInfo: Unit = {
    //    val jarName = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath()
    //    val fileseparator = System.getProperty("file.separator")
    //    val jarpath = jarName.substring(0, jarName.lastIndexOf(fileseparator))
    //    val xmlFile = XML.load(jarpath + "/conf/dataconf.xml")

    val xmlFile = XML.load("/Users/didi/work/scala-SDK-3.0.3-2.10/testdata" + "/conf/dataconf.xml")
    val dataList = (xmlFile \ "dataProperties" \ "data")

    dataList.foreach { data =>
      {
        val dataBean = new DataBean()
        val dataName = (data \ "dataName").text
        val outPutPath = (data \ "outPutPath").text
        val outPutFileDel_flg = (data \ "outPutFileDel_flg").text.toBoolean
        val lineCount = (data \ "lineCount").text.toLong
        val interval = (data \ "interval").text.toLong

        dataBean.setDataName(dataName)
        dataBean.setOutPutPath(outPutPath)
        dataBean.setOutPutFileDel_flg(outPutFileDel_flg)
        dataBean.setLineCount(lineCount)
        dataBean.setInterval(interval)
      }

    }

  }

  /**
   * 枚举类定义
   */
  object WeekDay extends Enumeration {
    // 为枚举类重新起个别名
    type surqWeekDay = Value
    // 最简单的定义
    //    val Mon, Tue, Wed, Thu, Fri, Sat, Sun = Value
    val Mon = Value(0, "start")
    val Tue = Value // 自动追加id =1
    val Wed = Value // 自动追加id =2
    val Thu = Value(10)
    val Fri = Value // 自动追加id =11
    val Sat = Value // 自动追加id =12
    val Sun = Value("end")
  }
}

class a  {

  outClass => class b(val nameNested: String) {
  def nestedtest() = {
    // 可以用outClass调用外部类一切public的东西
    println("内部类调用外部类测试：" + nameNested)
    Console println outClass.name
  }
  }
    val name:String=""
}