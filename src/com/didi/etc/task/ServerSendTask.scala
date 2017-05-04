package com.didi.etc.task

import java.io.BufferedOutputStream
import java.io.ObjectOutputStream

import com.didi.etc.WorkerNodeBean
import java.util.concurrent.LinkedBlockingQueue

class ServerSendTask(socketBean: WorkerNodeBean, newFileQueue: LinkedBlockingQueue[String]) extends Runnable {

  override def run() {

    val socket = socketBean.getSocket()
    val stream = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream))

    while (stream != null) {
      val filepath = newFileQueue.take
      stream.writeObject(filepath)
      stream.flush()
    }
  }
}