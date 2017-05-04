package com.didi.etc.prop

import com.didi.etc.prop.bean.OrderGPSBean
import org.apache.spark.Logging

/**
 * @author 宿荣全
 * @data 2017.1.6
 * 解析配置文件中的OrderGPS属性
 */
class OrderGPSProp(confName: String) extends BaseProp(confName) with Logging with Serializable {

  var hiveDatabase = ""
  var select = ""
  var where = ""
  var orderTable = ""
  var gpsDir = ""
  var resultDir = ""
  var appName = ""
  var areaID = ""
  var resutlUnionPathionFlg = false

  /**
   * 解析基础属性
   */
  def getOrderGPSPProp = {
    val orderGPSProperty = (xmlFile \ "OrderGPSProperty")

    hiveDatabase = (orderGPSProperty \ "HiveDatabase").text
    select = (orderGPSProperty \ "Select").text
    where = (orderGPSProperty \ "Where").text
    orderTable = (orderGPSProperty \ "OrderTable").text
    gpsDir = (orderGPSProperty \ "GPSDir").text
    resultDir = (orderGPSProperty \ "ResultDir").text
    appName = (orderGPSProperty \ "AppName").text
    areaID = (orderGPSProperty \ "AreaID").text
    resutlUnionPathionFlg = (orderGPSProperty \ "ResutlUnionPathionFlg").text.toBoolean

    val odb = new OrderGPSBean
    //OrderGPSP属性
    odb.setHiveDatabase(hiveDatabase)
    odb.setSelect(select)
    odb.setWhere(where)
    odb.setOrderTable(orderTable)
    odb.setGpsDir(gpsDir)
    odb.setResultDir(resultDir)
    odb.setAppName(appName)
    odb.setAreaID(areaID)
    odb.setResutlUnionPathionFlg(resutlUnionPathionFlg)
    odb
  }
  
  /**
   * 打印属app属性
   */
  override def printPorperties {
    super.printPorperties
    logInfo("============ OrderGPSP属性一览表 ==================")
    logInfo("hive 数据库:" + hiveDatabase)
    logInfo("hive 查询字段:" + select)
    logInfo("hive 查询条件:" + where)
    logInfo("hive 查询表名:" + orderTable)
    logInfo("GPS轨迹路径:" + gpsDir)
    logInfo("spark app名称:" + appName)
    logInfo("城市区域ID:" + areaID)
    logInfo("订单GPS轨迹输出总路径:" + resultDir)
    logInfo("是否合并成一个文件：" + resutlUnionPathionFlg)
  }
}