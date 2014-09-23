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
 * This application is based on the Apache Spark Example MovieLensALS. The original file may be found at
 * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/MovieLensALS.scala
 *
 * Copyright 2014 The Apache Software Foundation. Licensed under the Apache License, Version 2.0.
 *
 * This application uses MovieLens data set (http://grouplens.org/datasets/movielens/) collected by the
 * GroupLens Research Project at the University of Minnesota.
 */

package co.cask.cdap.moviesteer.app;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * MoveSteer: Movie Recommendation App based on Spark's MLibs using MovieLens dataset
 */
public class MovieSteerApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("MovieSteer");
    setDescription("Movie Recommendation App");
    addStream(new Stream("ratingsStream"));
    addFlow(new RatingsFlow());
    addSpark(new MovieSteerSpecification());
    addProcedure(new PredictionProcedure());

    try {
      ObjectStores.createObjectStore(getConfigurer(), "ratings", String.class);
      ObjectStores.createObjectStore(getConfigurer(), "predictions", String.class);
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectStore if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because String is an actual class.
      throw new RuntimeException(e);
    }
  }

  /**
   * A Spark Program that demonstrates the usage of Spark MLlib library
   */
  public static class MovieSteerSpecification extends AbstractSpark {
    @Override
    public SparkSpecification configure() {
      return SparkSpecification.Builder.with()
        .setName("MovieSteerProgram")
        .setDescription("MovieSteer Spark Program")
        .setMainClassName(MovieSteerProgram.class.getName())
        .build();
    }
  }

  /**
   * This Flowlet reads ratings from a Stream and saves them to a datastore.
   */
  public static class RatingReader extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(RatingReader.class);

    @UseDataSet("ratings")
    private ObjectStore<String> ratingsStore;

    @ProcessInput
    public void process(StreamEvent event) {
      String body = new String(event.getBody().array());
      LOG.trace("Ratings info: {}", body);
      ratingsStore.write(getIdAsByte(UUID.randomUUID()), body);
    }

    private static byte[] getIdAsByte(UUID uuid) {
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      return bb.array();
    }
  }

  /**
   * This is a simple Flow that consumes ratings from a Stream and stores them in a dataset.
   */
  public static class RatingsFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("RatingsFlow")
        .setDescription("Reads ratings information and stores in dataset")
        .withFlowlets()
        .add("reader", new RatingReader())
        .connect()
        .fromStream("ratingsStream").to("reader")
        .build();
    }
  }

  /**
   * Procedure that returns prediction based on user and movie parameters.
   */
  public static class PredictionProcedure extends AbstractProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(PredictionProcedure.class);

    // Annotation indicates that predictions dataset is used in the procedure.
    @UseDataSet("predictions")
    private ObjectStore<String> predictions;

    @Handle("getPrediction")
    public void getPrediction(ProcedureRequest request, ProcedureResponder responder)
      throws IOException, InterruptedException {
      String userId = request.getArgument("userId");
      if (userId == null) {
        responder.error(ProcedureResponse.Code.CLIENT_ERROR, "userId must be given as argument");
        return;
      }
      String movieId = request.getArgument("movieId");
      if (movieId == null) {
        responder.error(ProcedureResponse.Code.CLIENT_ERROR, "movieId must be given as argument");
        return;
      }
      byte[] key = ("" + userId + movieId).getBytes();

      String prediction = predictions.read(key);
      LOG.debug("got prediction {} for user {} and movie {}", prediction, userId, movieId);
      if (prediction == null) {
        responder.error(ProcedureResponse.Code.NOT_FOUND,
                        String.format("No prediction found for user %s and movie %s", userId, movieId));
        return;
      }

      responder.sendJson(prediction);
    }
  }
}
