package fr.efrei.extractionBatch.core

import fr.efrei.extractionBatch.utils.CarsUtils
import fr.efrei.extractionBatch.utils.CarsUtils.Cars
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._

object CarsMining {
  val pathToFile="data\\*"

  val conf = new SparkConf()
    .setAppName("Tweet mining")
    .setMaster("local[*]")

  val sc = SparkContext.getOrCreate(conf)
  def loadData(): RDD[Cars] = {


    sc.setLogLevel("off")

    sc.textFile(pathToFile)
      .mapPartitions(CarsUtils.parseFromJson(_))

  }

  def avgEngineTemperatureIsFailing() = {

    val rdd=loadData().filter(a=> a.isFailing)
    val countLine=rdd.count()
    if(countLine!=0) {
      rdd.map(a=>(a.engineTemperature.toFloat/countLine))
        .reduce((a,b)=>a+b)
    }

  }

  def avgEngineTemperatureIsMoving() = {

    val rdd=loadData().filter(a=> a.isMoving)
    val countLine=rdd.count()
    if(countLine!=0) {
      rdd.map(a => (a.engineTemperature.toFloat / countLine))
        .reduce((a, b) => a + b)
    }

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
    if(countFailing!=0){
      ((rdd.filter(a => a.fuelInTank==0)
      .count().toFloat)/countFailing)*100
    }
  }

  def newFailingCity()={
    val rdd=loadData().sortBy(x=>(x.vehiculeId,x.timestamp ))
    val a=rdd.zipWithIndex.map(kv => (kv._2, kv._1))
    val b=rdd.zipWithIndex.map(kv => (kv._2+1, kv._1))
    val s=a.join(b)
    val sortedOne=s.sortBy(s=>(s._1),false)
    val rdd4=sortedOne.union(sc.parallelize(sortedOne.take(1).map(x=>(x._1,(x._2._1,x._2._1)))))
    //permet de generer la derniere ligne

    val rdd2=rdd4.map(s=>(s._2._1,s._2._2))
    val rdd3=rdd2.map(x=>(getCityName(x._2.long,x._2.lat),returnMeInt(x._2,x._1)))
    rdd3.reduceByKey(_+_)
  }

  def returnMeInt(premier: Cars,deuxieme:Cars):Int ={
    if(premier.vehiculeId==deuxieme.vehiculeId && premier.timestamp == deuxieme.timestamp){
      return 1
    }// cas d'une ligne auto generee pour que la derniere ligne de notre fichier soit geree

    if(premier.vehiculeId!=deuxieme.vehiculeId){
      if(premier.isFailing){
        return 1
      }
      else{
        return 0
      }
    }
    else{
      return 0
    }
  }
}
