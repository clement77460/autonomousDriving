package fr.efrei.core

import fr.efrei.utils.CarsUtils
import fr.efrei.utils.CarsUtils.Cars
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd._


object CarsMining {
  val pathToFile = "data/sample.json"

  def loadData(): RDD[Cars] = {

    val conf = new SparkConf()
      .setAppName("Tweet mining")
      .setMaster("local[*]")

    val sc = SparkContext.getOrCreate(conf)

    sc.textFile(pathToFile)
      .mapPartitions(CarsUtils.parseFromJson(_))

  }

}
