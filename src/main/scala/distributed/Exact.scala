import org.rogach.scallop._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import ujson._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import shared.predictions._

package distributed {

class ExactConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val k = opt[Int](default=Some(10))
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String](default=Some("\t"))
  val master = opt[String]()
  val num_measurements = opt[Int](default=Some(1))
  verify()
}

object Exact {
  def main(args: Array[String]) {
    var conf = new ExactConf(args)

    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = conf.master.toOption match {
      case None => SparkSession.builder().getOrCreate();
      case Some(master) => SparkSession.builder().master(master).getOrCreate();
    }
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    println("")
    println("******************************************************")
    // conf object is not serializable, extract values that
    // will be serialized with the parallelize implementations
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()

    println("Loading training data from: " + conf.train())
    val train = loadSpark(sc, conf.train(), conf.separator(), conf.users(), conf.movies())
    val test = loadSpark(sc, conf.test(), conf.separator(), conf.users(), conf.movies())

    val measurements = (1 to scala.math.max(1,conf.num_measurements())).map(_ => timingInMs( () => {
      val predictor_allnn = predictorAllNN(train, sc)
      val predictor10NN = predictor_allnn(conf_k)
      MAE(test, predictor10NN)
    }))

    val timings = measurements.map(_._2)
    val mae = measurements(0)._1

    val users_avg = computeUsersAvg(train)
    val standardized_ratings = standardizeRatings(train, users_avg)
    val preprocessed_ratings = preprocessRatings(standardized_ratings)
    val similarities = parallelKNN(preprocessed_ratings, sc, conf_k)

    val predictor_allnn = predictorAllNN(train, sc)
    val predictor10NN = predictor_allnn(conf_k)

    val EK11 = similarities(0,0)
    val EK12 = similarities(0, 863)
    val EK13 = similarities(0, 885)
    val EK14 = predictor10NN(0, 0)
    val EK15 = predictor10NN(326, 1)
    val EK16 = MAE(test, predictor10NN)

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
          "Meta" -> ujson.Obj(
            "train" -> ujson.Str(conf.train()),
            "test" -> ujson.Str(conf.test()),
            "k" -> ujson.Num(conf.k()),
            "users" -> ujson.Num(conf.users()),
            "movies" -> ujson.Num(conf.movies()),
            "master" -> ujson.Str(sc.getConf.get("spark.master")),
            "num-executors" -> ujson.Str(if (sc.getConf.contains("spark.executor.instances"))
                                            sc.getConf.get("spark.executor.instances")
                                         else
                                            ""),
            "num_measurements" -> ujson.Num(conf.num_measurements())
          ),
          "EK.1" -> ujson.Obj(
            "1.knn_u1v1" -> ujson.Num(EK11),
            "2.knn_u1v864" -> ujson.Num(EK12),
            "3.knn_u1v886" -> ujson.Num(EK13),
            "4.PredUser1Item1" -> ujson.Num(EK14),
            "5.PredUser327Item2" -> ujson.Num(EK15),
            "6.Mae" -> ujson.Num(EK16)
          ),
          "EK.2" ->  ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          )
        )
        val json = write(answers, 4)
        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
    spark.stop()
  } 
}

}
