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
            "MinRentingDays" -> ujson.Num(0.0) // Datatype of answer: Double
          ),
          "E.2" -> ujson.Obj(
            "ContainerDailyCost" -> ujson.Num(0.0),
            "4RPisDailyCostIdle" -> ujson.Num(0.0),
            "4RPisDailyCostComputing" -> ujson.Num(0.0),
            "MinRentingDaysIdleRPiPower" -> ujson.Num(0.0),
            "MinRentingDaysComputingRPiPower" -> ujson.Num(0.0) 
          ),
          "E.3" -> ujson.Obj(
            "NbRPisEqBuyingICCM7" -> ujson.Num(0.0),
            "RatioRAMRPisVsICCM7" -> ujson.Num(0.0),
            "RatioComputeRPisVsICCM7" -> ujson.Num(0.0)
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
