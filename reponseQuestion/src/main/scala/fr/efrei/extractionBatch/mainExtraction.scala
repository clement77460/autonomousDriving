package fr.efrei.extractionBatch
import fr.efrei.extractionBatch.core.CarsMining


object mainExtraction {

  def main(args: Array[String]): Unit = {

    println(CarsMining.loadData().count())
    println(CarsMining.avgEngineTemperatureIsFailing())
    //println(CarsMining.avgEngineTemperatureIsMoving())
  }
}
