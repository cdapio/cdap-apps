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
 */

package co.cask.cdap.apps.movierecommender;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.spark.mllib.recommendation.Rating;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that exposes HTTP API to retrieve recommended movies.
 */
public class MovieRecommenderServiceHandler extends AbstractHttpServiceHandler {
  private static final Gson GSON = new Gson();

  @UseDataSet("recommendations")
  private ObjectStore<Rating> recommendations;

  @UseDataSet("ratings")
  private ObjectStore<UserScore> ratings;

  @UseDataSet("movies")
  private ObjectStore<String> movies;

  @Path("/recommend/{userId}")
  @GET
  public void recommend(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("userId") int userId) {
    byte[] userID = Bytes.toBytes(userId);

    CloseableIterator<KeyValue<byte[], UserScore>> userRatings =
      ratings.scan(userID, Bytes.stopKeyForPrefix(userID));
    try {
      if (!userRatings.hasNext()) {
        responder.sendError(HttpResponseStatus.NOT_FOUND.code(),
                            String.format("No ratings found for user %s.", userId));
        return;
      }

      CloseableIterator<KeyValue<byte[], Rating>> userPredictions =
        recommendations.scan(userID, Bytes.stopKeyForPrefix(userID));
      try {
        if (!userPredictions.hasNext()) {
          responder.sendError(HttpResponseStatus.NOT_FOUND.code(),
                              String.format("No recommendations found for user %s.", userId));
          return;
        }

        responder.sendJson(HttpResponseStatus.OK.code(), prepareResponse(movies, userRatings, userPredictions));
      } finally {
        userPredictions.close();
      }
    } finally {
      userRatings.close();
    }
  }

  /**
   * Prepares a json string of watched and recommended movies in the following format:
   * {"rated":["ratedMovie1","ratedMovie2"],"recommended":["recommendedMovie1","recommendedMovie2"]}
   *
   * @param userRatings user given rating to movies
   * @param userPredictions movie recommendation to user with predicted rating
   *
   * @return {@link com.google.gson.JsonObject} of watched and recommended movies
   */
  private JsonObject prepareResponse(ObjectStore<String> store,
                                     CloseableIterator<KeyValue<byte[], UserScore>> userRatings,
                                     CloseableIterator<KeyValue<byte[], Rating>> userPredictions) {
    JsonArray watched = new JsonArray();
    while (userRatings.hasNext()) {
      UserScore curRating = userRatings.next().getValue();
      watched.add(GSON.toJsonTree(store.read(Bytes.toBytes(curRating.getMovieID()))));
    }

    JsonArray recommended = new JsonArray();
    while (userPredictions.hasNext()) {
      Rating curPrediction = userPredictions.next().getValue();
      recommended.add(GSON.toJsonTree(store.read(Bytes.toBytes(curPrediction.product()))));
    }

    JsonObject response = new JsonObject();
    response.add("rated", watched);
    response.add("recommended", recommended);

    return response;
  }
}
