package org.apache.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, Milliseconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by chenfu_iwm on 16-5-19.
  */
class Test {
  private val sparkConf = new SparkConf().setMaster("local[4]").setAppName("test")
  private var ssc = new StreamingContext(sparkConf, Seconds(5))


  def test = {
    val topics = Set("basic1")
    KafkaHelper.setTopics(topics)
    val kafkaParams = Map(
      "metadata.broker.list" -> KafkaHelper.getBrokerList,
      "auto.offset.reset" -> "smallest"
    )

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics)
    stream.foreachRDD(rdd => {
      val a = rdd.take(2)
      a.foreach(println)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
object Test {
  def main(args: Array[String]): Unit = {
    val test = new Test
    test.test
  }
}
