/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * This example is based on the Apache Spark Example MovieLensALS. The original file may be found at
 * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/MovieLensALS.scala
 *
 * Copyright 2014 The Apache Software Foundation. Licensed under the Apache License, Version 2.0.
 *
 * This application uses MovieLens data set (http://grouplens.org/datasets/movielens/) collected by the
 * GroupLens Research Project at the University of Minnesota.
 */

package co.cask.cdap.moviesteer.app

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{ScalaSparkProgram, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Exception._

/**
 * MovieSteer Spark Program which uses Spark's MLib for making recommendations
 */
class MovieSteerProgram extends ScalaSparkProgram {
  private final val LOG: Logger = LoggerFactory.getLogger(classOf[MovieSteerProgram])

  case class Params(
                     numIterations: Int = 20,
                     lambda: Double = 1.0,
                     rank: Int = 10,
                     numUserBlocks: Int = -1,
                     numProductBlocks: Int = -1,
                     implicitPrefs: Boolean = false)

  override def run(sc: SparkContext) {
    LOG.info("Running with arguments {}", sc.getRuntimeArguments("args"))
    val params = parseArguments(sc, Params())
    LOG.info("Processing ratings data with parameters {}", params)

    val ratingsDataset: NewHadoopRDD[Array[Byte], String] =
      sc.readFromDataset("ratings", classOf[Array[Byte]], classOf[String])
    val lines = ratingsDataset.values
    val ratingData = lines.map { line =>
      val fields = line.split("::")
      if (params.implicitPrefs) {
        /*
         * MovieLens ratings are on a scale of 1-5:
         * 5: Must see
         * 4: Will enjoy
         * 3: It's okay
         * 2: Fairly bad
         * 1: Awful
         * So we should not recommend a movie if the recommended rating is less than 3.
         * To map ratings to confidence scores, we use
         * 5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5. This mappings means unobserved
         * entries are generally between It's okay and Fairly bad.
         * The semantics of 0 in this expanded world of non-positive weights
         * are "the same as never having interacted at all".
         */
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
      } else {
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }
    }.cache()

    val moviesDataset: NewHadoopRDD[Array[Byte], String] = sc.readFromDataset("movies", classOf[Byte], classOf[String])

    val numRatings = ratingData.count()
    val numUsers = ratingData.map(_.user).distinct().count()
    val numRatedMovies = ratingData.map(_.product).distinct().count()

    val numMovies = moviesDataset.count();

    println(s"Got $numRatings ratings from $numUsers users on $numRatedMovies movies out of $numMovies")

    val splits = ratingData.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = if (params.implicitPrefs) {
      /*
       * 0 means "don't know" and positive values mean "confident that the prediction should be 1".
       * Negative values means "confident that the prediction should be 0".
       * We have in this case used some kind of weighted RMSE. The weight is the absolute value of
       * the confidence. The error is the difference between prediction and either 1 or 0,
       * depending on whether r is positive or negative.
       */
      splits(1).map(x => Rating(x.user, x.product, if (x.rating > 0) 1.0 else 0.0))
    } else {
      splits(1)
    }.cache()

    LOG.info("Calculating model")

    val numTraining = training.count()
    val numTest = test.count()
    LOG.info(s"Training: $numTraining, test: $numTest.")

    ratingData.unpersist(blocking = false)

    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .setUserBlocks(params.numUserBlocks)
      .setProductBlocks(params.numProductBlocks)
      .run(training)

    val rmse = computeRmse(model, test, params.implicitPrefs)
    LOG.info(s"Test RMSE = $rmse.")

    LOG.debug("Creating prediction matrix")

    val userRatedMovies = ratingData.map(x => (x.user, x.product)).groupByKey().map(x => (x._1, x._2.toSet))

    val movies = moviesDataset.map(x => (Bytes.toString(x._1).toInt, x._2)).collect().toMap

    val originalContext: org.apache.spark.SparkContext = sc.getOriginalSparkContext.asInstanceOf[org.apache.spark.SparkContext]
    val notRatedMovies = userRatedMovies.map(x => (x._1, movies.keys.filter(!x._2.contains(_)).toSeq)).collect()

    for (curUser <- notRatedMovies) {
      var nr = originalContext.parallelize(curUser._2)
      var recom = model.predict(nr.map((curUser._1, _))).collect().sortBy(-_.rating).take(20)

      var i = 1
      println("Recommendations for User: " + curUser._1)
      recom.foreach { r =>
        println("%2d".format(i) + ": " + movies(r.product) + " with Rating: " + r.rating)
        i += 1
      }
    }
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean) = {

    def mapPredictedRating(r: Double) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

  /** Parse runtime arguments */
  def parseArguments(sc: SparkContext, defaultParams: Params): Params = {
    val args: Array[String] = sc.getRuntimeArguments("args")

    val numIterations = getInt(args, 0).getOrElse(defaultParams.numIterations)
    val lambda = getDouble(args, 1).getOrElse(defaultParams.lambda)
    val rank = getInt(args, 2).getOrElse(defaultParams.rank)
    val numUserBlocks = getInt(args, 3).getOrElse(defaultParams.numUserBlocks)
    val numProductBlocks = getInt(args, 4).getOrElse(defaultParams.numProductBlocks)
    val implicitPrefs = getBoolean(args, 5).getOrElse(defaultParams.implicitPrefs)

    Params(numIterations, lambda, rank, numUserBlocks, numProductBlocks, implicitPrefs)
  }

  def getInt(args: Array[String], idx: Int): Option[Int] = catching(classOf[Exception]).opt(args(idx).toInt)

  def getDouble(args: Array[String], idx: Int): Option[Double] = catching(classOf[Exception]).opt(args(idx).toDouble)

  def getBoolean(args: Array[String], idx: Int): Option[Boolean] = catching(classOf[Exception]).opt(args(idx).toBoolean)
}
