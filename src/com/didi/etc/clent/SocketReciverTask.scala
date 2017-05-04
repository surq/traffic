package com.didi.etc.clent

import java.io.IOException
import java.io.ObjectInputStream
import java.net.Socket
import java.net.UnknownHostException
import java.util.concurrent.LinkedBlockingQueue
import java.net.InetAddress
import com.didi.etc.PropertyBean
import com.didi.etc.LoadProperties

/**
 * @author 宿荣全
 * @date 2016.11.28
 * <p>client端发起请求从服务器端读取文件列表</p>
 */
class SocketReciverTask(newFileQueue: LinkedBlockingQueue[String]) extends Runnable {

  override def run() {
    val server = LoadProperties.propertyBean.getServerBean
    val port = server.getServerPort
    val serverIP = server.getServerIP
    var socket = new Socket(serverIP, port)
    var is = new ObjectInputStream(socket.getInputStream)
    
    while (true) {
      try {
        while (true) {
          val path = is.readObject().asInstanceOf[String]
          newFileQueue.offer(path)
        }

      } catch {
        case ex: UnknownHostException => ex.printStackTrace
        case e1: IOException          => e1.printStackTrace
        case e: Exception             => e.printStackTrace
      } finally {
        if (is != null) is.close
        if (socket != null) socket.close
        socket = new Socket(serverIP, port)
        is = new ObjectInputStream(socket.getInputStream)
        println("关闭socket连接，重新自动创建连接......")
      }
    }
  }
}