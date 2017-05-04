package com.didi.etc.clent

import com.didi.etc.LoadProperties
import java.io.IOException
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.net.Socket
import java.net.UnknownHostException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Executors
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration

/**
 * @author 宿荣全
 * @date 2016.11.28
 * <p>client端发起请求从服务器端读取文件列表</p>
 */
object ClentServer {

  // 属性配置
  val propertyBean = LoadProperties.initPro
  val client = LoadProperties.propertyBean.getClientBean
  val threadNum = client.getProcessThreads
  val newFileQueue = new LinkedBlockingQueue[String]
  val delFileQueue = new LinkedBlockingQueue[Tuple2[Int, String]]

  // 两个根线程
  val executorPool = Executors.newFixedThreadPool(threadNum + 2)
  def main(args: Array[String]): Unit = {
    // 启动 socket client端追加新文件线程
    executorPool.execute(new SocketReciverTask(newFileQueue))
    // 对读取完的源文件进行处理
    executorPool.execute(new CompletedFileTask(delFileQueue))
    while (true) {
      val srcFileName = newFileQueue.take()
      // 读取文件并处理
      executorPool.execute(new FilePorcessTask(delFileQueue, srcFileName))
    }
  }
}