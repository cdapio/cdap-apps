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
import com.google.common.base.Splitter;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.spark.mllib.recommendation.Rating;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Handler that exposes HTTP API to retrieve recommended movies.
 */
public class MovieRecommendServiceHandler extends AbstractHttpServiceHandler {
  private static final String PARAM_USER_ID = "userId";

  @UseDataSet("recommendations")
  private ObjectStore<Rating> recommendations;

  @UseDataSet("ratings")
  private ObjectStore<UserScore> ratings;

  @UseDataSet("movies")
  private ObjectStore<String> movies;

  @Path("getrecommendation")
  @GET
  public void getRecommendation(HttpServiceRequest request,
                                HttpServiceResponder responder) throws IOException, InterruptedException {
    String userId = null;
    byte[] userID;

    // Parse the userId parameter
    try {
      URI uri = new URI(request.getRequestURI());
      String query = uri.getQuery();
      if (null != query) {
        Map<String, String> params = Splitter.on('&').trimResults().withKeyValueSeparator("=").split(query);
        userId = params.get(PARAM_USER_ID);
      }
      if (null == userId) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST.code(), "Parameter userId must be given in URI.");
        return;
      } else {
        userID = Bytes.toBytes(Integer.parseInt(userId));
      }
    } catch (NumberFormatException e) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.code(), "Value of userId must be a correct number.");
      return;
    } catch (Exception e) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.code(), "Parameter userId must be given in correct URI.");
      return;
    }

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
