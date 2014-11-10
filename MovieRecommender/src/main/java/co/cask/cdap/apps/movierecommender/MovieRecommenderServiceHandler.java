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
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.spark.mllib.recommendation.Rating;

import java.util.Iterator;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that exposes HTTP API to retrieve recommended movies.
 */
public class MovieRecommenderServiceHandler extends AbstractHttpServiceHandler {
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

    Iterator<KeyValue<byte[], UserScore>> userRatings = ratings.scan(userID, Bytes.stopKeyForPrefix(userID));
    if (!userRatings.hasNext()) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.code(), String.format("No ratings found for user %s.", userId));
      return;
    }

    Iterator<KeyValue<byte[], Rating>> userPredictions = recommendations.scan(userID, Bytes.stopKeyForPrefix(userID));
    if (!userPredictions.hasNext()) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.code(),
                          String.format("No recommendations found for user %s.", userId));
      return;
    }

    responder.sendJson(HttpResponseStatus.OK.code(),
                       MovieRecommenderHelper.prepareResponse(movies, userRatings, userPredictions));
  }
}
