package com.didi.etc.its.tool.decrypt

import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import com.didi.etc.decode.JiNanKakouDecode
import scala.io.Source

object JiNanEventCountDecrypt {

  def main(args: Array[String]): Unit = {

    // TODO
    val scanDir = ""
    // 满足条件的是否处理
    val filter_flg = true
    // hdfs 文件过滤规则
    val filterRegex = ""
    // 打描周期
    val scanInterval = 2000

    val aeskey = "1,3,16,13,5,7"
    
    val thNum = 5
    
    // 预处理的新文件
    val newFileQueue = new LinkedBlockingQueue[String]()
    val fineshFileQueue = new LinkedBlockingQueue[String]()
    val executorPool = Executors.newCachedThreadPool()
    executorPool.execute(new scanner(newFileQueue, scanDir, filter_flg, filterRegex, scanInterval))
   for (i <- 0 until thNum) executorPool.execute(new JiNandecrypt(newFileQueue,fineshFileQueue, aeskey))
    executorPool.execute(new CompletionTasker(fineshFileQueue))
  }
}