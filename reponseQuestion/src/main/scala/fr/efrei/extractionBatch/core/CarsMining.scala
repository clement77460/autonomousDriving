package fr.efrei.extractionBatch.core

import fr.efrei.extractionBatch.utils.CarsUtils
import fr.efrei.extractionBatch.utils.CarsUtils.Cars
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._

object CarsMining {
  val pathToFileReal = "data/sample.json"
  val pathToFile = "data/value.json"
  val pathTo="C:\\projetScala\\sparkStreaming\\data\\*"

  def loadData(): RDD[Cars] = {

    val conf = new SparkConf()
      .setAppName("Tweet mining")
      .setMaster("local[*]")

    val sc = SparkContext.getOrCreate(conf)

    sc.textFile(pathTo)
      .mapPartitions(CarsUtils.parseFromJson(_))

  }

  def avgEngineTemperatureIsFailing() = {

    val rdd=loadData().filter(a=> a.isFailing)
    val countLine=rdd.count()
    rdd.map(a=>(a.engineTemperature/countLine))
      .reduce((a,b)=>a+b)

  }

  def avgEngineTemperatureIsMoving() = {

    val rdd=loadData().filter(a=> a.isMoving)
    val countLine=rdd.count()
    rdd.map(a=>(a.engineTemperature/countLine))
      .reduce((a,b)=>a+b)

  }
}
