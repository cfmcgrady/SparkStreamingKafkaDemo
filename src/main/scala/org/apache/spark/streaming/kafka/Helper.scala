package org.apache.spark.streaming.kafka

import streaming.utils.NginxUtils

import scala.util.Random

/**
  * Created by chenfu_iwm on 16-5-19.
  */
object KafkaHelper {
  private var kafkaTestUtils = new KafkaTestUtils()
  private var topics_ : Set[String] = _
  kafkaTestUtils.setup()
  def setTopics(topics: Set[String]) {
    topics_ = topics
    topics.foreach { t =>
      kafkaTestUtils.createTopic(t)
    }
    sendMessage()
  }

  private def sendMessage(): Unit = {
    val r = new Runnable {
      override def run(): Unit = {
        while (true) {
          val data = produceData
          topics_.foreach { t =>
            kafkaTestUtils.sendMessages(t, data)
          }
          Thread.sleep(1000)
        }
      }
    }

    new Thread(r).start

  }

  def getBrokerList: String = {
    kafkaTestUtils.brokerAddress
  }

  private def produceData: Array[String] = {
    val pages = List("/index", "/download", "/upload", "/help", "/software")
    val r = Random.nextInt(5)
    val msg = (0 to r).map( x => {
      val page = pages(Random.nextInt(pages.length - 1))
      (NginxUtils.produceLog(page))
    }).toArray
    msg
  }
}
