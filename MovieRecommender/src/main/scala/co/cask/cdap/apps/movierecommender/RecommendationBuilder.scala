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
 * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/
 * MovieLensALS.scala
 *
 * Copyright 2014 The Apache Software Foundation. Licensed under the Apache License, Version 2.0.
 *
 * This application uses MovieLens data set (http://grouplens.org/datasets/movielens/) collected by the
 * GroupLens Research Project at the University of Minnesota.
 */

package co.cask.cdap.apps.movierecommender

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{ScalaSparkProgram, SparkContext}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.NewHadoopRDD
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Exception._

/**
 * Spark Program which makes recommendation for movies to users
 */
class RecommendationBuilder extends ScalaSparkProgram {
  private final val LOG: Logger = LoggerFactory.getLogger(classOf[RecommendationBuilder])

  case class Params(
                     numIterations: Int = 20,
                     lambda: Double = 1.0,
                     rank: Int = 10,
                     numUserBlocks: Int = -1,
                     numProductBlocks: Int = -1,
                     implicitPrefs: Boolean = false)

  override def run(sc: SparkContext) {
    LOG.info("Running with arguments {}", sc.getRuntimeArguments.get("args"))
    val params = parseArguments(sc, Params())
    LOG.info("Processing ratings data with parameters {}", params)

    val ratingsDataset: NewHadoopRDD[Array[Byte], Text] = sc.readFromStream("ratingsStream", classOf[Text])
    val userScores = ratingsDataset.values

    val usRDD = userScores.map { e =>
      val userScore = e.toString.split("::")
      new UserScore(userScore(0).toInt, userScore(1).toInt, userScore(2).toInt)
    }.cache()
    val scores = usRDD.collect()

    val ratingData = userScores.map { curUserScore =>
      val userScore = curUserScore.toString.split("::")
      if (params.implicitPrefs) {
        /*
        * MovieLens ratings are on a scale of 1-5:
        * To map ratings to confidence scores, we subtract 2.5
        * 5 -> 2.5
        */
        Rating(userScore(0).toInt, userScore(1).toInt, userScore(2).toDouble - 2.5)
      } else {
        Rating(userScore(0).toInt, userScore(1).toInt, userScore(2).toDouble)
      }
    }.cache()

    val originalContext: org.apache.spark.SparkContext = sc.getOriginalSparkContext.
      asInstanceOf[org.apache.spark.SparkContext]
    val parallelizedScores = originalContext.parallelize(scores)

    val scoresRDD = parallelizedScores.keyBy(x => Bytes.add(Bytes.toBytes(x.getUserID), Bytes.toBytes(x.getMovieID)))
    sc.writeToDataset(scoresRDD, "ratings", classOf[Array[Byte]], classOf[UserScore])

    val moviesDataset: NewHadoopRDD[Array[Byte], String] = sc.readFromDataset("movies", classOf[Array[Byte]],
      classOf[String])

    val numRatings = ratingData.count()
    val numUsers = ratingData.map(_.user).distinct().count()
    val numRatedMovies = ratingData.map(_.product).distinct().count()

    val numMovies = moviesDataset.count()

    println(s"Got $numRatings ratings from $numUsers users on $numRatedMovies movies out of $numMovies")


    LOG.info("Calculating model")

    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .setUserBlocks(params.numUserBlocks)
      .setProductBlocks(params.numProductBlocks)
      .run(ratingData)

    LOG.debug("Creating predictions")

    val userRatedMovies = ratingData.map(x => (x.user, x.product)).groupByKey().map(x => (x._1, x._2.toSet))

    val movies = moviesDataset.map(x => (Bytes.toInt(x._1), x._2)).collect().toMap
    val notRatedMovies = userRatedMovies.map(x => (x._1, movies.keys.filter(!x._2.contains(_)).toSeq)).collect()

    for (curUser <- notRatedMovies) {
      var nr = originalContext.parallelize(curUser._2)
      var recom = originalContext.parallelize(model.predict(nr.map((curUser._1, _)))
        .collect().sortBy(-_.rating).take(20))

      var recomRDD = recom.keyBy(x => Bytes.add(Bytes.toBytes(x.user), Bytes.toBytes(x.product))).
        map(x => (x._1, new UserScore(x._2.user, x._2.product, x._2.rating.toInt)))

      sc.writeToDataset(recomRDD, "recommendations", classOf[Array[Byte]], classOf[UserScore])
    }

    LOG.debug("Stored predictions in dataset. Done!")
  }

  /** Parse runtime arguments */
  def parseArguments(sc: SparkContext, defaultParams: Params): Params = {
    val arguments: String = sc.getRuntimeArguments.get("args")
    val args: Array[String] = if (arguments == null) Array() else arguments.split("\\s")


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
