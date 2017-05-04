package com.didi.etc.task

import java.io.IOException
import java.net.ServerSocket
import java.util.concurrent.LinkedBlockingQueue
import com.didi.etc.WorkerNodeBean
import com.didi.etc.PropertyBean
import com.didi.etc.LoadProperties
import java.net.InetSocketAddress

/**
 * @author 宿荣全
 * @date 2016.11.28
 * <p>接收client端发起请求并存储client的想关信息</p>
 */
class RegistedServerTask(socketList: LinkedBlockingQueue[WorkerNodeBean]) extends Runnable {

  override def run() {
    try {
      //配置文件获取
      val server = LoadProperties.propertyBean.getServerBean
      val port = server.getServerPort
      val ServerSocket = new ServerSocket(port)
      while (true) {
        val client = ServerSocket.accept()
        val remoteInfo = client.getRemoteSocketAddress
        val machineInfo = remoteInfo.asInstanceOf[InetSocketAddress]
        val machineAddress = machineInfo.getAddress
        // 封装socket 信息
        val workerNode = new WorkerNodeBean()
        workerNode.setSocket(client)
        workerNode.setHostName(machineAddress.getHostName)
        workerNode.setHostIp( machineAddress.getHostAddress)
        workerNode.setPort(machineInfo.getPort)
        workerNode.setWorkInfo("WorkNode实例信息［" + workerNode.getHostName() + " " + workerNode.getHostIp() + ":" + workerNode.getPort() + "]")
        socketList.offer(workerNode)
        println(workerNode.getWorkInfo() + "已经注册为worker！")
      }
    } catch {
      case ex: IOException => ex.printStackTrace()
      case e: Exception    => e.printStackTrace()
    }
  }
}