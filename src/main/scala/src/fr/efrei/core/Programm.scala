import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext, sql}


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Programm {

  def main(ars:Array[String])={
    sparkStreaming()

  }

  def sparkStreaming()={
    println("start sparkStreaming")
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("Simple Streaming Application")
    val ssc = new StreamingContext(conf, Seconds(4))


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


    stream.map(record => (record.key, record.value)).foreachRDD(_.foreach{x =>
      println(x)
      println("############################################################################################################")
      println("############################################################################################################")
      println("############################################################################################################")})

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    println("end sparkStreaming")
  }
}
