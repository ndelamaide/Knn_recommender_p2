package shared

import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext

import org.apache.spark.rdd.RDD // TO REMOVE NOT IN MILESTONE 2
import scala.collection.mutable.ListBuffer

package object predictions
{
  // ------------------------ For template
  case class Rating(user: Int, item: Int, rating: Double)

  def timingInMs(f : ()=>Double ) : (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end-start)/1000000.0)
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0

  def std(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else { 
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble) 
    }
  }


  def load(path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = Source.fromFile(path)
    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies) 
    for (line <- file.getLines) {
      val cols = line.split(sep).map(_.trim)
      toInt(cols(0)) match {
        case Some(_) => builder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
        case None => None
      }
    }
    file.close
    builder.result()
  }

  def loadSpark(sc : org.apache.spark.SparkContext,  path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = sc.textFile(path)
    val ratings = file
      .map(l => {
        val cols = l.split(sep).map(_.trim)
        toInt(cols(0)) match {
          case Some(_) => Some(((cols(0).toInt-1, cols(1).toInt-1), cols(2).toDouble))
          case None => None
        }
      })
      .filter({ case Some(_) => true
                 case None => false })
      .map({ case Some(x) => x
             case None => ((-1, -1), -1) }).collect()

    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies)
    for ((k,v) <- ratings) {
      v match {
        case d: Double => {
          val u = k._1
          val i = k._2
          builder.add(u, i, d)
        }
      }
    }
    return builder.result
  }

  def partitionUsers (nbUsers : Int, nbPartitions : Int, replication : Int) : Seq[Set[Int]] = {
    val r = new scala.util.Random(1337)
    val bins : Map[Int, collection.mutable.ListBuffer[Int]] = (0 to (nbPartitions-1))
       .map(p => (p -> collection.mutable.ListBuffer[Int]())).toMap
    (0 to (nbUsers-1)).foreach(u => {
      val assignedBins = r.shuffle(0 to (nbPartitions-1)).take(replication)
      for (b <- assignedBins) {
        bins(b) += u
      }
    })
    bins.values.toSeq.map(_.toSet)
  }
  
/*----------------------------------------Utils----------------------------------------------------------*/

  def scale(x: Double, useravg: Double): Double = {
    if (x > useravg)
      5 - useravg
    else if (x < useravg)
      useravg - 1
    else
      1
  }

  def standardize(rating: Double, userAvg: Double): Double = {
      (rating - userAvg) / scale(rating, userAvg)
  }

  def matrixMultiplication(matrix1: CSCMatrix[Double], matrix2: CSCMatrix[Double]): CSCMatrix[Double] = {

      val builder = new CSCMatrix.Builder[Double](rows=matrix1.rows, cols=matrix2.cols)

      for (col2 <- 0 to matrix2.cols-1) {

        val vec_result = matrix1 * matrix2(0 to matrix2.rows-1, col2)

        for ((r, v) <- vec_result.activeIterator) {
          builder.add(r, col2, v)
        }
      }

      return builder.result 
  }

/*--------------------------------------- BR --------------------------------------------------*/

  // Rows are users, Columns are movies

  def computeGlobalAvg(ratings: CSCMatrix[Double]): Double =  {
    sum(ratings) / ratings.activeSize
  }

  def computeUsersAvg(ratings: CSCMatrix[Double]): DenseVector[Double] = {

    val ones_cols = DenseVector.ones[Double](ratings.cols)

    val user_sum = ratings * ones_cols
    val counts = ratings.mapActiveValues(x => 1.0) * ones_cols

    return user_sum /:/ counts // element-wise division
  }

  def standardizeRatings(ratings: CSCMatrix[Double], users_avg: DenseVector[Double]): CSCMatrix[Double] = {
    
    val builder = new CSCMatrix.Builder[Double](rows=ratings.rows, cols=ratings.cols)

    for ((k, v) <- ratings.activeIterator) {
        builder.add(k._1, k._2, standardize(v, users_avg(k._1)))
    }

    return builder.result
  }

  def preprocessRatings(standardized_ratings: CSCMatrix[Double]): CSCMatrix[Double] = {

    val ones_cols = DenseVector.ones[Double](standardized_ratings.cols)

    val ru_squared = standardized_ratings *:* standardized_ratings
    val sum_ru_squared = (ru_squared * ones_cols).mapActiveValues(scala.math.sqrt(_))

    val builder = new CSCMatrix.Builder[Double](rows=standardized_ratings.rows, cols=standardized_ratings.cols)

    for ((k, v) <- standardized_ratings.activeIterator) {
      builder.add(k._1, k._2, v / sum_ru_squared(k._1))
    }

    return builder.result
  } 


  def computeUserSimilarities(preprocessed_ratings: CSCMatrix[Double], k: Int): CSCMatrix[Double] = {

    var similarities = matrixMultiplication(preprocessed_ratings, preprocessed_ratings.t)

    val builder = new CSCMatrix.Builder[Double](rows=similarities.rows, cols=similarities.cols)

    for (u <- 0 to similarities.rows - 1) {
      // Set self similarity to 0
      similarities(u, u) = 0.0

      val similar_u = similarities(0 to similarities.rows-1, u)
      for (i <- argtopk(similar_u, k)) {

        builder.add(u, i, similar_u(i)) 
      }
    }

    return builder.result
  }

  def computeRi_(ratings: CSCMatrix[Double] , standardized_ratings: CSCMatrix[Double], user_similarities: CSCMatrix[Double]): CSCMatrix[Double] = {
    
    val numerator = matrixMultiplication(user_similarities, standardized_ratings)
    val denominator = matrixMultiplication(user_similarities.mapActiveValues(scala.math.abs(_)), ratings.mapActiveValues(x => 1.0))

    var result = new CSCMatrix.Builder[Double](rows=ratings.rows, cols=ratings.cols)

    for ((k,v) <- numerator.activeIterator) {
      val denominator_k = denominator(k)
      if (denominator_k != 0.0) result.add(k._1, k._2, v/denominator_k)
    }   

    return result.result
  }

    /**
    * Predictor for any k-nearest neighboors
    *
    * @param ratings
    * @return a predictor for any k
    */
  def predictorAllNN(ratings: CSCMatrix[Double]): Int => (Int, Int) => Double = {

    val global_avg = computeGlobalAvg(ratings)
    val users_avg = computeUsersAvg(ratings)
    val standardized_ratings = standardizeRatings(ratings, users_avg)
    val preprocessed_ratings = preprocessRatings(standardized_ratings)

    (k: Int) => {

      val similarities = computeUserSimilarities(preprocessed_ratings, k)

      val Ris = computeRi_(ratings, standardized_ratings, similarities)
      
        (u: Int, i: Int) =>  {

          val ru = users_avg(u)

          if (ru != 0.0) {

            val ri = Ris(u, i)

            ru + ri * scale(ru + ri, ru)

          } else {

            global_avg
          }          
        }
    }
  }

  /** 
    * Computes the MAE of a given predictor
    * 
    * @param test_ratings ratings to compute the MAE on
    * @param predictor the predictor used to make the predictions
    * @return the value of the MAE
    */
  def MAE(test_ratings: CSCMatrix[Double], predictor: (Int, Int) => Double): Double = {
    
    var sum = 0.0

    for ((k, v) <- test_ratings.activeIterator) {
      sum += scala.math.abs(v - predictor(k._1, k._2))
    }

    sum / test_ratings.activeSize
  }


  /*--------------------------------------- EK --------------------------------------------------*/
  
  def computeUserSimilaritiesParallel(preprocessed_ratings: CSCMatrix[Double], u: Int): DenseVector[Double] = {

    val ratings_u = preprocessed_ratings.t(0 to preprocessed_ratings.cols-1, u)

    var result = new DenseVector((preprocessed_ratings * ratings_u).toArray)
    result(u) = 0 // remove self similarity
    return result
  }

  def parallelKNN(preprocessed_ratings: CSCMatrix[Double], sc: org.apache.spark.SparkContext, k: Int): CSCMatrix[Double] = {

    val br = sc.broadcast(preprocessed_ratings)

    def topk(u: Int): (Int, IndexedSeq[(Int, Double)]) = {

      val ratings_ = br.value
      val su = computeUserSimilaritiesParallel(ratings_, u)
      return (u, argtopk(su, k).map(v => (v, su(v))))
    }

    val topks = sc.parallelize(0 to preprocessed_ratings.rows-1).map(topk(_)).collect()

    val builder = new CSCMatrix.Builder[Double](rows=preprocessed_ratings.rows, cols=preprocessed_ratings.rows)

    for ((user, topk) <- topks) {
      for ((other_user, similarity) <- topk) {
        builder.add(user, other_user, similarity)
      }
    }

    return builder.result
  }

  /**
    * Predictor for any k-nearest neighboors
    *
    * @param ratings
    * @return a predictor for any k
    */
  def predictorAllNN(ratings: CSCMatrix[Double], sc: org.apache.spark.SparkContext): Int => (Int, Int) => Double = {

    val global_avg = computeGlobalAvg(ratings)
    val users_avg = computeUsersAvg(ratings)
    val standardized_ratings = standardizeRatings(ratings, users_avg)
    val preprocessed_ratings = preprocessRatings(standardized_ratings)

    (k: Int) => {

      val similarities = parallelKNN(preprocessed_ratings, sc, k)

      val Ris = computeRi_(ratings, standardized_ratings, similarities)
      
        (u: Int, i: Int) =>  {

          val ru = users_avg(u)

          if (ru != 0.0) {

            val ri = Ris(u, i)

            ru + ri * scale(ru + ri, ru)

          } else {

            global_avg
          }          
        }
    }
  }
  /*--------------------------------------- Approximate --------------------------------------------------*/



  def parallelKNNHelper(preprocessed_ratings: CSCMatrix[Double], sc: org.apache.spark.SparkContext, k: Int): Seq[(Int, (Int, Double))] = {

    val br = sc.broadcast(preprocessed_ratings)

    def topk(u: Int): (Int, IndexedSeq[(Int, Double)]) = {

      val ratings_ = br.value
      val su = computeUserSimilaritiesParallel(ratings_, u)
      return (u, argtopk(su, k).map(v => (v, su(v))))
    }

    val topks = sc.parallelize(0 to preprocessed_ratings.rows-1).map(topk(_)).collect()

    for {
      (user, topk) <- topks
      (other_user, similarity) <- topk
      if (user != other_user)
    } yield (user, (other_user, similarity))
  }


  def parallelKNNApproximate(preprocessed_ratings: CSCMatrix[Double], sc: org.apache.spark.SparkContext, k: Int, partitioned_users: Seq[Set[Int]]): CSCMatrix[Double] = {
    
    val preprocessed_ratings_t = preprocessed_ratings.t

    // Divide the ratings in corresponding partitions
    val partitioned_ratings = partitioned_users.map(partition => {

      val builder = new CSCMatrix.Builder[Double](rows=preprocessed_ratings.rows, cols=preprocessed_ratings.cols)

      for (user_partition <- partition) {
        val user_ratings = preprocessed_ratings_t(0 to preprocessed_ratings_t.rows-1, user_partition)

        for ((k, v) <- user_ratings.activeIterator) {
          builder.add(user_partition, k, v)
        }
      }

      builder.result
    })
    
    // Compute similarities for each partition and merge partitions
    val key_sim_pairs = partitioned_ratings.flatMap((partition => parallelKNNHelper(partition, sc, k))).groupBy(_._1).mapValues(list_ratings => 
                        list_ratings.map(x => x._2).groupBy(_._1).map(x => x._2.maxBy(_._2)).toArray.sortBy(-_._2).slice(0, k))

    val builder = new CSCMatrix.Builder[Double](rows=preprocessed_ratings.rows, cols=preprocessed_ratings.rows)

    for ((user, list_sim) <- key_sim_pairs) {
      for ((other_user, sim) <- list_sim) {
        builder.add(user, other_user, sim)
      }
    }

    return builder.result

  }


  def predictorAllNNApproximate(ratings: CSCMatrix[Double], sc: org.apache.spark.SparkContext, partitioned_users: Seq[Set[Int]]): Int => (Int, Int) => Double = {

    val global_avg = computeGlobalAvg(ratings)
    val users_avg = computeUsersAvg(ratings)
    val standardized_ratings = standardizeRatings(ratings, users_avg)
    val preprocessed_ratings = preprocessRatings(standardized_ratings)

    (k: Int) => {

      val similarities = parallelKNNApproximate(preprocessed_ratings, sc, k, partitioned_users)

      val Ris = computeRi_(ratings, standardized_ratings, similarities)
      
        (u: Int, i: Int) =>  {

          val ru = users_avg(u)

          if (ru != 0.0) {

            val ri = Ris(u, i)

            ru + ri * scale(ru + ri, ru)

          } else {

            global_avg
          }          
        }
    }
  }
}


