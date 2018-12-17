package fr.efrei.sparkStreaming

import com.google.gson._

object CarsUtils{
	case class Cars (
		lat : String,
		long : String,
		vehiculeId : String,
		isFailing : Boolean,
		temperature : Int,
		engineTemperature : Int,
		fuelInTank : Int,
		isMoving : Boolean,
		timeStamp: Long
		)

	
	def parseFromJson(lines:Iterator[String]):Iterator[Cars] = {
		val gson = new Gson
		lines.map(line => gson.fromJson(line, classOf[Cars]))
	}
}
