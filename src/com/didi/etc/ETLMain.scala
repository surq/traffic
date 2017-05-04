package com.didi.etc

import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

import com.didi.etc.task.RegistedServerTask
import com.didi.etc.task.ServerStartSendTask
import com.didi.etc.task.ScannerTask
/**
 * @author 宿荣全
 * @date 2016.11.28
 * <p>读取文件列表，根据文件内容的时间戳，折分文件</p>
 */
object ETLMain {
//val logger = LoggerFactory.getLogger("ETLMain")
  // 属性配置
  val propertyBean = LoadProperties.initPro

  // 预处理的新文件
  val newFileQueue = new LinkedBlockingQueue[String]()
  // worker node 列表
  val socketList = new LinkedBlockingQueue[WorkerNodeBean]()
  val executorPool = Executors.newCachedThreadPool()

  def main(args: Array[String]): Unit = {
//  logger.info("ETCMain：启动服务SocketServer监听前来注册的workernode－－－完成！")
    //--------------------socket Server 向worker结点分发新文件路径---------------------------
    // 启动服务SocketServer监听注册workerＮode
    executorPool.execute(new RegistedServerTask(socketList))
    println("ETCMain：启动服务SocketServer监听前来注册的workernode－－－完成！")
    // 启动每一个注册的workerＮode socket线程,并向其发送newFileQueue中的新文件
    executorPool.execute(new ServerStartSendTask(socketList, executorPool, newFileQueue))
    println("ETCMain：启动每一个注册的worker socket线程,并向其发送新增文件列表 －－－完成！")
    executorPool.execute(new ScannerTask(newFileQueue))
    println("ETCMain：启动文件夹打描线程 －－－完成！")
    //------------------------------------------------------------------------------------
  }
}