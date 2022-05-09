package test.optimizing

import breeze.linalg._
import breeze.numerics._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import shared.predictions._
import test.shared.helpers._

class OptimizingTests extends AnyFunSuite with BeforeAndAfterAll {
  
   val separator = "\t"
   val train2Path = "data/ml-100k/u2.base"
   val test2Path = "data/ml-100k/u2.test"
   var train2 : CSCMatrix[Double] = null
   var test2 : CSCMatrix[Double] = null

   override def beforeAll {
       // For these questions, train2 and test are collected in a scala Array
       // to not depend on Spark
       train2 = load(train2Path, separator, 943, 1682)
       test2 = load(test2Path, separator, 943, 1682)
   }

   // Provide tests to show how to call your code to do the following tasks.
   // Ensure you use the same function calls to produce the JSON outputs in
   // the corresponding application.
   // Add assertions with the answer you expect from your code, up to the 4th
   // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).
   test("kNN predictor with k=10") {

    val users_avg = computeUsersAvg(train2)

    val k = 10

    val standardized_ratings = standardizeRatings(train2, users_avg)
    val preprocessed_ratings = preprocessRatings(standardized_ratings)
    val similarities = computeUserSimilarities(preprocessed_ratings, k)

    val predictor_allnn = predictorAllNN(train2)
    val predictor10NN = predictor_allnn(k)

     // Similarity between user 1 and itself
     assert(within(similarities(0,0), 0.0, 0.0001))
 
     // Similarity between user 1 and 864
     assert(within(similarities(0, 863), 0.2423, 0.0001))

     // Similarity between user 1 and 886
     assert(within(similarities(0, 885), 0.0, 0.0001))

     // Prediction user 1 and item 1
     assert(within(predictor10NN(0, 0), 4.3190, 0.0001))

     // Prediction user 327 and item 2
     assert(within(predictor10NN(326, 1), 2.6994, 0.0001))

     // MAE on test2
     assert(within(MAE(test2, predictor10NN), 0.8287, 0.0001)) 
   } 
}
