package com.didi.etc.task

import java.io.BufferedOutputStream
import java.io.ObjectOutputStream
import java.util.concurrent.LinkedBlockingQueue

import com.didi.etc.WorkerNodeBean
import java.util.concurrent.ExecutorService

class ServerStartSendTask(
    socketList: LinkedBlockingQueue[WorkerNodeBean],
    executorPool: ExecutorService,
    newFileQueue:LinkedBlockingQueue[String]) extends Runnable {

  override def run() {
    // TODO
	  Thread.sleep(30000)
    while (true) {
      val socketInfo = socketList.take()
      executorPool.execute(new ServerSendTask(socketInfo,newFileQueue))
      println("ServerStartSendTask:（来自客户端对接请求）："+socketInfo.getWorkInfo())
    }
  }
}