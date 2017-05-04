package com.didi.etc.its.common.util

import kafka.producer.{ ProducerConfig, Producer }
import java.util.Properties
import scala.collection.mutable
import kafka.producer.KeyedMessage
import scala.collection.mutable.ArrayBuffer
import scala.beans.BeanProperty

object KafkaProducer extends Serializable {
  
  @BeanProperty var brokers =""
    @BeanProperty var topic =""
  val producermap = new mutable.HashMap[String, Producer[String, String]]()
  def getProducer(brokers: String): Producer[String, String] = {
    val producer = producermap.getOrElse(brokers, {
      val props = getProducerConfig(brokers)
      val pro = new Producer[String, String](new ProducerConfig(props))
      producermap.put(brokers, pro)
      pro
    })
    producer
  }
  def getProducerConfig(brokers: String): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props
  }

  def sendList(msgList: List[String], brokers: String, topic: String): Unit = {
    if (msgList.size > 0) {
     println("=============msgList:"+msgList.size)
      val message = ArrayBuffer[KeyedMessage[String, String]]()
      msgList.foreach { msg => message += new KeyedMessage[String, String](topic, msg.split("\t")(0), msg) }
      val producer = getProducer(brokers)
      producer.send(message: _*)
    }
  }

  def sparkSendKafka(it: Iterator[String], brokers: String, topic: String) = {
    val list = it.toList
    sendList(list, brokers, topic)
    list.toIterator
  }
}