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
    sc.setLogLevel("off")

    sc.textFile(pathToFile)
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
  
  def getCityName(long:Float,lat:Float):String={
	  if (lat >= (48.666667) && lat <=(49.066667) && long<=(2.533333) && long >=(2.133333)){
		return "Paris"
	  }
	  if (lat >= (37.5749295) && lat <=(37.9749295) && long<=(-122.21941550000001) && long >=(-122.61941550000001)){
		return "San Francisco"
	  }
	  
		return "Other"
  }
  
  def failingCity() ={
	  loadData().filter(a=> a.isFailing)
      .map(a => (getCityName(a.long,a.lat),1))
		  .reduceByKey( _ +_ )
		  .sortByKey(false)
  }
  
  def failingByFuel() ={
	  val rdd=loadData().filter(a=> a.isFailing)
    val countFailing=rdd.count()
    ((rdd.filter(a => a.fuelInTank==0)
    .count().toFloat)/countFailing)*100
  }
}
