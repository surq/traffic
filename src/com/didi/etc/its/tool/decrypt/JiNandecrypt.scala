package com.didi.etc.its.tool.decrypt

import com.didi.etc.decode.JiNanKakouDecode
import java.util.concurrent.LinkedBlockingQueue
import scala.io.Source

class JiNandecrypt(newFileQueue: LinkedBlockingQueue[String],
                   fineshFileQueue: LinkedBlockingQueue[String],
                   aeskey: String) extends Runnable {

  override def run() {

   while (true){
     
     val path = newFileQueue.take()
     // TODO hdfs
     val lines = Source.fromFile(path).getLines().filter(_.trim() != "")
     val jncode = new JiNanKakouDecode
     val valuelines = jncode.decode(lines, aeskey, "UTF-8")
     fineshFileQueue.offer(path)
   }

  }

}