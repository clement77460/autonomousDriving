package fr.efrei.extractionBatch.core

import fr.efrei.extractionBatch.utils.CarsUtils
import fr.efrei.extractionBatch.utils.CarsUtils.Cars
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._


object CarsMining2 {
  val pathToFile = "data/sample.json"

  def loadData(): RDD[Cars] = {

    val conf = new SparkConf()
      .setAppName("Tweet mining")
      .setMaster("local[*]")

    val sc = SparkContext.getOrCreate(conf)

    sc.textFile(pathToFile)
      .mapPartitions(CarsUtils.parseFromJson(_))

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
  

}
