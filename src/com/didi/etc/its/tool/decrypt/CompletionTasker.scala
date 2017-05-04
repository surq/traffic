package com.didi.etc.its.tool.decrypt

import java.util.concurrent.LinkedBlockingQueue

class CompletionTasker(newFileQueue: LinkedBlockingQueue[String]) extends Runnable {

  override def run() {
    while (true) {
      val path = newFileQueue.take()
    }

  }
}