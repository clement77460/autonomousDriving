package fr.efrei.sparkStreaming
import java.util.Properties

import com.google.gson.Gson
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

object KafkaProducerObject {

  def launchMe(): Unit ={
    val props=new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG,"Kafka example")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    val producer=new KafkaProducer[String,String](props)
    val gson=new Gson()
    val data=new ProducerRecord[String,String]("test","key1","{\"lat\":48.866667,\"long\":2.333333,\"vehiculeId\":\"b10\",\"temperature\":20,\"engineTemperature\":36,\"timestamp\":1544358561968,\"isMoving\":false,\"isFailing\":true}")
    producer.send(data)
    producer.send(data)
    producer.close()
  }

}
