package shared

import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext

import org.apache.spark.rdd.RDD // TO REMOVE NOT IN MILESTONE 2

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
  
//-----------------------------------------------Milestone1---------------------------------------------------
/*----------------------------------------Utils----------------------------------------------------------*/

  def scale(x: Double, useravg: Double): Double = {
    if (x > useravg)
      5 - useravg
    else if (x < useravg)
      useravg - 1
    else
      1
  }

  /** 
    * Computes global average
    * 
    * @param ratings
    * @return Global average
    */
  def computeGlobalAvg(ratings: Array[Rating]): Double = {
    mean(ratings.map(x => x.rating))
  }

  /** 
    * Computes average rating of every user in ratings
    * 
    * @param ratings
    * @return Map with key-value pairs (user, avg-rating)
    */
  def computeUsersAvg(ratings: Array[Rating]): Map[Int, Double] = {
    ratings.map(x => (x.user, x.rating)).groupBy(_._1).mapValues(x => mean(x.map(y => y._2)))
  }

  /** 
    * Computes average rating of every item in ratings
    * 
    * @param ratings
    * @return Map with key-value pairs (item, avg-rating)
    */
  def computeItemsAvg(ratings: Array[Rating]): Map[Int, Double] = {
    ratings.groupBy(_.item).mapValues(x => mean(x.map(y => y.rating)))
  }

  /**
    * Computes global average deviation of each item 
    * 
    * @param ratings
    * @param users_avg dictionary of user avegage ratings (user, avg-rating)
    * @return Map with key-value pairs (item, global average dev)
    */
  def computeItemsGlobalDev(standardized_ratings: Array[Rating], users_avg: Map[Int, Double]): Map[Int, Double] = {
    standardized_ratings.groupBy(_.item).mapValues(x => mean(x.map(y => y.rating)))
  }
  
  /** 
    * Standardizes all ratings in ratings dataframe 
    * 
    * @param ratings
    * @param users_avg dictionary of user avegage ratings (user, avg-rating)
    * @return Dataframe with standardized ratings
    */
  // def standardizeRatings(ratings: Array[Rating], users_avg: Map[Int, Double]): Array[Rating] = {
  //    ratings.map(x => Rating(x.user, x.item, standardize(x.rating, users_avg(x.user))))
  //  }
  
  /** 
    * Computes the MAE of a given predictor
    * 
    * @param test_ratings ratings to compute the MAE on
    * @param predictor the predictor used to make the predictions
    * @return the value of the MAE
    */
  def MAE(test_ratings: Array[Rating], predictor: (Int, Int) => Double): Double = {
    mean(test_ratings.map(x => scala.math.abs(x.rating - predictor(x.user, x.item))))
  }
  
  /*----------------------------------------Global variables----------------------------------------------------------*/

  var global_avg: Double = 0.0
  var users_avg: Map[Int,Double] = null
  var items_avg: Map[Int, Double] = null
  var global_avg_devs: Map[Int, Double] = null
  var standardized_ratings: Array[Rating] = null
  var similarities_uniform: Map[(Int, Int),Double] = null
  var similarities_cosine: Map[(Int, Int),Double] = null
  var preprocessed_ratings: Array[Rating] = null
  var user_similarities: Map[Int,Array[(Int, Double)]] = null
  var preprocessed_groupby_user: Map[Int,Array[Rating]] = null
  var standardized_groupby_item: Map[Int,Array[Rating]] = null

  /*----------------------------------------Baseline----------------------------------------------------------*/
  
  /**
    * Predictor using the global average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorGlobalAvg(ratings: Array[Rating]): (Int, Int) => Double = {
    //val global_avg = mean(ratings.map(x => x.rating))
    (u: Int, i: Int) => global_avg
  }
  
  /**
    * Predictor using the user average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorUserAvg(ratings: Array[Rating]): (Int, Int) => Double = {

    (u: Int, i: Int) => users_avg.get(u) match {
      case Some(x) => x
      case None => global_avg
    }
  }
  
  /**
    * Predictor using the item average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorItemAvg(ratings: Array[Rating]): (Int, Int) => Double = {

    (u: Int, i: Int) => items_avg.get(i) match {
      case Some(x) => x
      case None => users_avg.get(u) match {
        case Some(x) => x
        case None => global_avg
      }
    }
  }

  /**
    * Predictor using the baseline
    *
    * @param ratings
    * @return a predictor
    */
  def predictorRating(ratings: Array[Rating]): (Int, Int) => Double = {

    (u: Int, i: Int) =>  {

      users_avg.get(u) match {
        case Some(x) => {
          val ri = global_avg_devs.get(i) match {
            case Some(x) => x
            case None => 0.0
          }
          x + ri * scale(x + ri, x)
        }
        case None => global_avg
      }
    }
  }

  /*----------------------------------------Spark----------------------------------------------------------*/

  /*---------Helpers---------*/

  def standardize(rating: Double, userAvg: Double): Double = {
      (rating - userAvg) / scale(rating, userAvg)
  }
  
   /** 
    * Computes global average
    * 
    * @param ratings
    * @return Global average
    */
  def computeGlobalAvg(ratings: RDD[Rating]): Double = {
    ratings.map(x => x.rating).mean()
  }

  /** 
    * Computes average rating of every user in ratings
    * 
    * @param ratings
    * @return Map with key-value pairs (user, avg-rating)
    */
  def computeUsersAvg(ratings: RDD[Rating]): Map[Int, Double] = {
    ratings.map(x => (x.user, (x.rating, 1))).reduceByKey((v1, v2) => 
      (v1._1 + v2._1, v1._2 + v2._2)).mapValues(pair => pair._1 / pair._2).collect().toMap
  }
  
  /** 
    * Computes average rating of every item in ratings
    * 
    * @param ratings
    * @return Map with key-value pairs (item, avg-rating)
    */
  def computeItemsAvg(ratings: RDD[Rating]): Map[Int, Double] = {
    ratings.map(x => (x.item, (x.rating, 1))).reduceByKey((v1, v2) => 
      (v1._1 + v2._1, v1._2 + v2._2)).mapValues(pair => pair._1 / pair._2).collect().toMap
  }
  
  /**
    * Computes global average deviation of each item 
    * 
    * @param ratings
    * @param users_avg dictionary of user avegage ratings (user, avg-rating)
    * @return Map with key-value pairs (item, global average dev)
    */
  def computeItemsGlobalDev(ratings: RDD[Rating], users_avg: Map[Int, Double]) : Map[Int, Double] = {
    ratings.map(x => (x.item, (standardize(x.rating, users_avg(x.user)), 1))).reduceByKey((v1, v2) =>
      (v1._1 + v2._1, v1._2 + v2._2)).mapValues(pair => pair._1 / pair._2).collect().toMap
  }

  /** 
    * Computes the MAE of a given predictor
    * 
    * @param test_ratings ratings to compute the MAE on
    * @param predictor the predictor used to make the predictions
    * @return the value of the MAE
    */
  def MAE(test_ratings: RDD[Rating], predictor: (Int, Int) => Double): Double = {
      test_ratings.map(x => scala.math.abs(x.rating - predictor(x.user, x.item))).mean()
  }

  /*---------Predictors---------*/
  
  /**
    * Predictor using the global average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorGlobalAvg(ratings: RDD[Rating]): (Int, Int) => Double = {
    //val global_avg = ratings.map(x => x.rating).mean()
    (u: Int, i: Int) => global_avg
  }
 
  /**
    * Predictor using the user average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorUserAvg(ratings: RDD[Rating]): (Int, Int) => Double = {

    (u: Int, i: Int) => users_avg.get(u) match {
      case Some(x) => x
      case None => global_avg
    }
  }
  
  /**
    * Predictor using the item average
    *
    * @param ratings
    * @return a predictor
    */
  def predictorItemAvg(ratings: RDD[Rating]): (Int, Int) => Double = {
  

    (u: Int, i: Int) => items_avg.get(i) match {
      case Some(x) => x
      case None => users_avg.get(u) match {
        case Some(y) => y
        case None => global_avg
      }
    }
  }

  /**
    * Predictor using the baseline
    *
    * @param ratings
    * @return a predictor
    */
  def predictorRating(ratings: RDD[Rating]): (Int, Int) => Double = {

    (u: Int, i: Int) =>  {
      users_avg.get(u) match {
        case Some(x) => {
          val ri = global_avg_devs.get(i) match {
            case Some(x) => x
            case None => 0.0
          }
          x + ri * scale(x + ri, x)
        }
        case None => global_avg
      }
    }
  }

  /*----------------------------------------Personalized----------------------------------------------------------*/


  /**
    * Pre-process the ratings before computing the similarity
    *
    * @param standardized_ratings
    * @return a dataframe with the ratings pre-processed
    */
  // def preprocessRatings(standardized_ratings: Array[Rating]): Array[Rating] = {

  //   // Compute sum of square of devs for each user
  //   val squared_sum_users = standardized_ratings.groupBy(_.user).mapValues(x => x.foldLeft(0.0)((sum, rating) => sum + scala.math.pow(rating.rating, 2)))

  //   standardized_ratings.map(x => Rating(x.user, x.item, x.rating / scala.math.sqrt(squared_sum_users(x.user))))
  // }
  
  /**
    * Computes the similarity between each user using a value of 1 (uniform)
    *
    * @param ratings
    * @return a dictionary of key-value pairs ((user1, user2), similarity)
    */
  def computeSimilaritiesUniform(ratings: Array[Rating]): Map[(Int, Int), Double] = {
    val user_set = preprocessed_groupby_user.keySet

    val sims = for {
      u1 <- user_set
      u2 <- user_set
    } yield ((u1, u2), 1.0)

    sims.toMap
  }

  /**
    * Predictor using a similarity of 1 between each user (uniform)
    *
    * @param ratings
    * @return a predictor
    */
  def predictorUniform(ratings: Array[Rating]): (Int, Int) => Double = {

    (u: Int, i: Int) =>  {

      // Faster if pre-compute everything ?
      val ratings_i = standardized_ratings.filter(x => x.item == i)

      val ri_numerator = if (ratings_i.isEmpty) 0.0 else ratings_i.map(x => {
        similarities_uniform.get((x.user, u)) match {
          case Some(y) => y * x.rating 
          case None => 0.0
        }
      }).sum

      val ri_denominator =  ratings_i.map(x => {
        similarities_uniform.get((x.user, u)) match {
          case Some(y) => y
          case None => 0.0
        }
      }).sum

      users_avg.get(u) match {
        case Some(x) => {
          val ri = if (ri_denominator == 0.0) 0.0 else ri_numerator / ri_denominator
          x + ri * scale(x + ri, x)
        }
        case None => global_avg
      }
    }
  }

  /**
    * Computes the adjusted cosine similarity between two users
    *
    * @param preprocessed_ratings
    * @param u first user
    * @param v secoond user
    * @return the cosine similarity between the two users
    */
  def adjustedCosine(groupby_user: Map[Int,Array[Rating]], u: Int, v: Int): Double = {
    if (u == v) 1.0
    else {
      val ratings_u_v = groupby_user(u) ++ groupby_user(v)
      ratings_u_v.groupBy(_.item).filter(x => x._2.length == 2).mapValues(x => x.foldLeft(1.0)((mult, rating) => mult * rating.rating)).values.sum
    }
  }

  /**
    * Computes the cosine similarity for all pairs of users
    *
    * @param preprocessed_ratings
    * @return a dictionary of key-value pairs ((user1, user2), similarity)
    */
  def computeCosine(preprocessed_ratings: Array[Rating]): Map[(Int, Int), Double] = {

   val user_set = preprocessed_groupby_user.keySet

   val user_pairs = (for(u <- user_set; v <- user_set if u < v) yield (u, v))

   //val groupby_user = preprocessed_ratings.groupBy(_.user)
   
   user_pairs.map(x => (x, adjustedCosine(preprocessed_groupby_user, x._1, x._2))).toMap
  }

  /**
    * Computes the user-specific weighted sum deviation using the cosine similarity
    *
    * @param cosine_similarities
    * @param usr the user
    * @param itm the item
    * @return the user-specific weighted sum deviation of item itm for user usr
    */
  def computeRiCosine(cosine_similarities: Map[(Int, Int), Double], usr: Int, itm: Int): Double = {
    
    val ratings_i = standardized_groupby_item.getOrElse(itm, Array[Rating]())
    
    val similarities = ratings_i.map(x => {
      if (x.user == usr) (x.user, 1.0)
      else cosine_similarities.get(if (x.user < usr) (x.user, usr) else (usr, x.user)) match {
        case Some(y) => (x.user, y)
        case None => (x.user, 0.0)
      }
    }).toMap

    val numerator = if (ratings_i.isEmpty) 0.0 else ratings_i.map(x => similarities(x.user) * x.rating).sum

    val denominator = if (similarities.isEmpty) 0.0 else similarities.mapValues(x => scala.math.abs(x)).values.sum

    if (denominator == 0.0) 0.0 else numerator / denominator
  }

  /**
    * Predictor using the cosine similarity
    *
    * @param ratings
    * @return a predictor
    */
  def predictorCosine(ratings: Array[Rating]): (Int, Int) => Double = {

    (u: Int, i: Int) =>  {

      users_avg.get(u) match {
        case Some(x) => {
          val ri = computeRiCosine(similarities_cosine, u, i)
          x + ri * scale(x + ri, x)
        }
        case None => global_avg
      }
    }
  }

  //------------------------------------------------------KNN-------------------------------------------------------------

  
  def computeUserSimilarities(ratings: Array[Rating]): Map[Int,Array[(Int, Double)]] = {
    val user_set = preprocessed_groupby_user.keySet
    user_set.map(x => {
      (x , (for (u <- user_set if (u != x)) yield (u, if (x < u) similarities_cosine((x, u)) else similarities_cosine((u, x)))).toArray.sortBy(-_._2))
    }).toMap
  }
  /**
    * Predictor for any k-nearest neighboors
    *
    * @param ratings
    * @return a predictor for any k
    */
  def predictorAllNN(ratings: Array[Rating]): Int => (Int, Int) => Double = {

    (k: Int) => {

      val k_user_similarities = user_similarities.mapValues(x => x.slice(0, k))
      
        (u: Int, i: Int) =>  {

          users_avg.get(u) match {
            case Some(ru) => {

              // Similarity values of k similar users to u
              val k_user_similar_map = k_user_similarities(u).toMap

              // Ratings of item i by k similar users
              val rating_i_k_users = standardized_groupby_item.getOrElse(i, Array[Rating]()).filter(x => k_user_similar_map.keySet.contains(x.user))
              
              // Keep among the k similar users those who have rated i
              val k_users_rating_i = rating_i_k_users.map(x => x.user).distinct.map(x => (x, k_user_similar_map(x))).toMap

              val numerator = if (rating_i_k_users.isEmpty) 0.0 else rating_i_k_users.map(x => x.rating * k_users_rating_i(x.user)).sum
              val denominator = if (rating_i_k_users.isEmpty) 0.0 else k_users_rating_i.mapValues(x => scala.math.abs(x)).values.sum

              val ri = if (denominator == 0.0) 0.0 else numerator / denominator

              ru + ri * scale(ru + ri, ru)

            }

            case None => global_avg
          }
            
        }
    }
  }



  //------------------------------------------------Milestone2----------------------------------------- 
  /** 
//     * Computes global average
//     * 
//     * @param ratings
//     * @return Global average
//     */
//   def computeGlobalAvg(ratings: CSCMatrix[Rating]): Double = {

//     for ((k,v) <- x.activeIterator) {
//       val row = k._1
//       val col = k._2
//       // Do something with row, col, v
// }
//     mean(ratings.map(x => x.rating))
//   }

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

        val ones_rows = DenseVector.ones[Double](standardized_ratings.rows)

        val ru_squared = standardized_ratings *:* standardized_ratings
        val sum_ru_squared = (ru_squared.t * ones_rows).mapValues(x => scala.math.sqrt(x))

        val builder = new CSCMatrix.Builder[Double](rows=standardized_ratings.rows, cols=standardized_ratings.cols)

        for ((k, v) <- standardized_ratings.activeIterator) {
            builder.add(k._1, k._2, v / sum_ru_squared(k._1))
        }

        return builder.result
      }

      // Probably wrong as its not fast
      def computeUserSimilarities(preprocessed_ratings: CSCMatrix[Double]): CSCMatrix[Double] = {

        val builder = new CSCMatrix.Builder[Double](rows=preprocessed_ratings.rows, cols=preprocessed_ratings.rows)

        for (u1 <- 0 to (preprocessed_ratings.rows - 1); u2 <- 0 to (preprocessed_ratings.rows - 1)) {
          if (u1 < u2) {
            builder.add(u1, u2, 
            sum(preprocessed_ratings(u1, 0 to (preprocessed_ratings.cols - 1)) *:* preprocessed_ratings(u2, 0 to (preprocessed_ratings.cols - 1))))
          } else if (u1 == u2) {
            builder.add(u1, u2, 1.0)
          }
        }

        return builder.result
      }

      def computeRi(standardized_ratings: CSCMatrix[Double], user_similarities: CSCMatrix[Double], user: Int, item: Int): Double = {
        
        val r_v = standardized_ratings(0 to standardized_ratings.rows, item)

        var numerator = 0.0
        var denominator = 0.0

        for ((k, v) <- r_v.activeIterator) {
           val s_uv = if (user < k) user_similarities(user, k) else user_similarities(k, user)

           numerator += s_uv * v
           denominator += scala.math.abs(s_uv)
        } 

        if (denominator == 0.0) 0.0 else numerator / denominator
      }



  /* ----------------------------------- Parallel k-NN ----------------------------------------- */
  


  // def parallelKNN(ratings: Array[Rating], sc: org.apache.spark.SparkContext, k: Int): CSCMatrix[Double] = {

  //     val users_avg = computeUsersAvg(ratings)
  //     val standardized_ratings = standardizeRatings(ratings, users_avg)
  //     val preprocessed_ratings = preprocessRatings(standardized_ratings)

  //     val br = sc.broadcast(preprocessed_ratings)

  //     def topk(u: Int): (Int, (Int, Double)) = {

  //       val r_curve = br.value
  //       val su = 
  //     }




  //     val builder = new CSCMatrix.Builder[Double](rows=2, cols=2) // ex

  //     return builder.result

  // }


}


