import org.rogach.scallop._
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import ujson._

package economics {

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)

    // E.1
    val price_icc = 38600
    val renting_icc = 20.40
    val E1 = price_icc / renting_icc

    // E.2
    val container_GB_s = 1.6e-7
    val container_vCPU_s = 1.14e-6

    val container_GB_day = container_GB_s * 86400
    val container_vCPU_day = container_vCPU_s * 86400

    // 4 * 8GB of ram + cost renting cpu 1 day
    val E21 = 4 * 8 * container_GB_day + container_vCPU_day //ContainerDailyCost

    val price_RPi = 108.48
    val energy_cost = 0.25 // CHF / kWh

    val power_idle = 3
    val power_computing = 4

    val E22 = 4 * (power_idle * 1e-3 * energy_cost * 24) //4RPisDailyCostIdle
    val E23 = 4 * (power_computing * 1e-3 * energy_cost * 24) //4RPisDailyCostComputing
    val E24 = price_RPi / (E21 - E22)
    val E25 = price_RPi / (E21 - E23)

    // E.3
    val E31 = (price_icc / price_RPi).floor //NbRPisEqBuyingICCM7

    val ram_RPis = E31 * 8
    val ram_ICC = 24 * 64

    val E32 = ram_RPis / ram_ICC 

    // Throughput 1 vCPU = 4 RPis
    val E33 = E31 / 4



    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach{
        f => try{
          f.write(content)
        } finally{ f.close }
    }
    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {

        val answers = ujson.Obj(
          "E.1" -> ujson.Obj(
            "MinRentingDays" -> ujson.Num(E1) // Datatype of answer: Double
          ),
          "E.2" -> ujson.Obj(
            "ContainerDailyCost" -> ujson.Num(E21),
            "4RPisDailyCostIdle" -> ujson.Num(E22),
            "4RPisDailyCostComputing" -> ujson.Num(E23),
            "MinRentingDaysIdleRPiPower" -> ujson.Num(E24),
            "MinRentingDaysComputingRPiPower" -> ujson.Num(E25) 
          ),
          "E.3" -> ujson.Obj(
            "NbRPisEqBuyingICCM7" -> ujson.Num(E31),
            "RatioRAMRPisVsICCM7" -> ujson.Num(E32),
            "RatioComputeRPisVsICCM7" -> ujson.Num(E33)
          )
        )

        val json = write(answers, 4)
        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}

}
