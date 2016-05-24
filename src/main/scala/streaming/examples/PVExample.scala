package streaming.examples

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{KafkaUtils, KafkaHelper}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import streaming.utils.NginxUtils

object PVExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("test")
    var ssc = new StreamingContext(sparkConf, Seconds(5))


    val topics = Set("logs")
    KafkaHelper.setTopics(topics)
    val kafkaParams = Map(
      "metadata.broker.list" -> KafkaHelper.getBrokerList,
      "auto.offset.reset" -> "smallest"
    )

    val logs = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
    logs.map {
      case (key, value) => {
        NginxUtils.parse(value)
      }
    }.filter {
      _ != None
    }.map(nginx => {
      (nginx.get.URL, 1)
    }).reduceByKey(_ + _).print(3)
    ssc.start()
    ssc.awaitTermination()
  }
}
