package com.didi.etc.its.tool

import scala.xml.XML
import com.didi.etc.prop.bean.AutoConfBean
import java.io.File
import com.didi.util.auto.OrderGPSAutoUtil

object OrderGPSAuto {

  // 模板要修改的字段
  val list = List("Type", "Where", "GPSDir", "AreaID", "ResultDir", "AppName")

  def main(args: Array[String]): Unit = {

    // 读取配置问件，如果指定则读入指定的配置文件，否则默认conf.xml
    var confFileName = ""
    if (args.size == 2) confFileName = args(0) else {
      Console println "请输入自动化工具的配置文件！"
      return
    }
    //调度平台自动追加的：yyyyMMddHH
    val orderGPSAutoUtil = new OrderGPSAutoUtil(args(1))
    //读取自动化脚本配置文件属性
    val autoConfBean = orderGPSAutoUtil.anlysisProp(confFileName)
    // 读取Sys自动化配置文件
    val sysConfList = orderGPSAutoUtil.getHDFSFileConf(autoConfBean.getSysConfPath)

    val tax_tmp_confPath = autoConfBean.getTaxiXml_templete
    val gulf_tmp_confPath = autoConfBean.getGulfXml_templete
    val shScreptPath = autoConfBean.getOrderShellPath

    // 删除上次生成的shell脚本
    (new File(shScreptPath + "/*")).delete
    // 自动生成taxi的OrderGps xml配置文件
    val shTaxiList = orderGPSAutoUtil.cretateXMLAndSH(tax_tmp_confPath, autoConfBean, sysConfList, list)
    // 自动生成gulf的OrderGps xml配置文件
    val shGulfList = orderGPSAutoUtil.cretateXMLAndSH(gulf_tmp_confPath, autoConfBean, sysConfList, list)
    val orderShellList = shTaxiList ::: shGulfList

    //合并所有单独的脚本,集合到一个脚本中去后台执行
    orderGPSAutoUtil.mergerShell(orderShellList, shScreptPath + "/" + "autoOrderGPS.sh")
  }
}