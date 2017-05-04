package com.didi.etc.prop.bean

import scala.beans.BeanProperty
/**
 * 系统配置，上下游依赖的部分
 */
class AutoSysConfBean {

  //  任务名称,MM_PATH,多边形区域,多边形ID,城市ID,城市名称,多边形区域GPS_PATH,订单GPS_PATH
  @BeanProperty var taskName = ""
  @BeanProperty var mm_Path = ""
  @BeanProperty var polygon = ""
  @BeanProperty var polygonID = ""
  @BeanProperty var cityID = ""
  @BeanProperty var cityName = ""
  @BeanProperty var polygonGps_Path = ""
  @BeanProperty var orderGps_Path = ""
}