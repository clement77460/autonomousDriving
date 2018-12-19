package fr.efrei.extractionBatch
import fr.efrei.extractionBatch.core.CarsMining


object mainExtraction {

  def main(args: Array[String]): Unit = {

//    println(CarsMining.loadData().count())
    println("Avg Engine Temperature Failing: "+CarsMining.avgEngineTemperatureIsFailing())
    println("Avg Engine Temperature Moving: "+CarsMining.avgEngineTemperatureIsMoving())
	
	  println("Failing City: "+CarsMining.failingCity().foreach(println))
	  println("Failing Fuel: "+CarsMining.failingByFuel()+"%")
  }
}
