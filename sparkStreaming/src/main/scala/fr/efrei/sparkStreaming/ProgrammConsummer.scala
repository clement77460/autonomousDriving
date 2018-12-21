package fr.efrei.sparkStreaming


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object ProgrammConsummer {

  def main(ars:Array[String])={
    sparkStreaming()
  }

  def sparkStreaming()={
    val timeRecord=1
    println("start sparkStreaming")
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("Simple Streaming Application")
      .set("spark.files.overwrite","true")

    val ssc = new StreamingContext(conf, Seconds(timeRecord))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.value)).saveAsTextFiles("../reponseQuestion/data/carRecord","json")


    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
