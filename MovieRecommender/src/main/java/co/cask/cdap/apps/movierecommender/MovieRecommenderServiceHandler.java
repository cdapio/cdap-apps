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

import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that exposes HTTP API to retrieve recommended movies.
 */
public class MovieRecommenderServiceHandler extends AbstractHttpServiceHandler {
  public static final String RECOMMEND = "recommend";
  @UseDataSet("recommendations")
  private ObjectStore<UserScore> recommendations;

  @UseDataSet("ratings")
  private ObjectStore<UserScore> ratings;

  @UseDataSet("movies")
  private ObjectStore<String> movies;

  @Path("/" + RECOMMEND + "/{userId}")
  @GET
  public void recommend(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("userId") int userId) {
    byte[] userID = Bytes.toBytes(userId);

    CloseableIterator<KeyValue<byte[], UserScore>> userRatings =
      ratings.scan(userID, Bytes.stopKeyForPrefix(userID));
    try {
      if (!userRatings.hasNext()) {
        responder.sendError(HttpURLConnection.HTTP_NOT_FOUND,
                            String.format("No ratings found for user %s.", userId));
        return;
      }

      CloseableIterator<KeyValue<byte[], UserScore>> userPredictions =
        recommendations.scan(userID, Bytes.stopKeyForPrefix(userID));
      try {
        if (!userPredictions.hasNext()) {
          responder.sendError(HttpURLConnection.HTTP_NOT_FOUND,
                              String.format("No recommendations found for user %s.", userId));
          return;
        }

        responder.sendJson(getRecommendation(movies, userRatings, userPredictions));
      } finally {
        userPredictions.close();
      }
    } finally {
      userRatings.close();
    }
  }

  /**
   * Prepares a response of watched and recommended movies in the following format:
   * {"rated":["ratedMovie1","ratedMovie2"],"recommended":["recommendedMovie1","recommendedMovie2"]}
   *
   * @param store dataset with stored movies
   * @param userRatings user given rating to movies
   * @param userPredictions movie recommendation to user with predicted rating
   *
   * @return {@link Recommendation} of watched and recommended movies
   */
  private Recommendation getRecommendation(ObjectStore<String> store,
                                           CloseableIterator<KeyValue<byte[], UserScore>> userRatings,
                                           CloseableIterator<KeyValue<byte[], UserScore>> userPredictions) {
    Recommendation recommendations = new Recommendation();

    while (userRatings.hasNext()) {
      recommendations.addRated(store.read(Bytes.toBytes(userRatings.next().getValue().getMovieID())));
    }

    while (userPredictions.hasNext()) {
      recommendations.addRecommended(store.read(Bytes.toBytes(userPredictions.next().getValue().getMovieID())));
    }

    return recommendations;
  }
}
