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

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.spark.mllib.recommendation.Rating;

import java.io.IOException;
import java.util.Iterator;

/**
 * Procedure that returns movie recommendations for users
 */
public class RecommendMovieProcedure extends AbstractProcedure {

  private static final Gson GSON = new Gson();

  @UseDataSet("recommendations")
  private ObjectStore<Rating> recommendations;

  @UseDataSet("ratings")
  private ObjectStore<UserScore> ratings;

  @UseDataSet("movies")
  private ObjectStore<String> movies;

  @Handle("getRecommendation")
  public void getRecommendation(ProcedureRequest request, ProcedureResponder responder)
    throws IOException, InterruptedException {

    String userId = request.getArgument("userId");
    if (userId == null) {
      responder.error(ProcedureResponse.Code.CLIENT_ERROR, "userId must be given as argument");
      return;
    }

    byte[] userID = Bytes.toBytes(Integer.parseInt(userId));

    Iterator<KeyValue<byte[], UserScore>> userRatings = ratings.scan(userID, Bytes.stopKeyForPrefix(userID));
    if (!userRatings.hasNext()) {
      responder.error(ProcedureResponse.Code.NOT_FOUND, String.format("No ratings found for user %s", userId));
      return;
    }

    Iterator<KeyValue<byte[], Rating>> userPredictions = recommendations.scan(userID, Bytes.stopKeyForPrefix(userID));
    if (!userPredictions.hasNext()) {
      responder.error(ProcedureResponse.Code.NOT_FOUND, String.format("No recommendations found for user %s", userId));
      return;
    }

    responder.sendJson(ProcedureResponse.Code.SUCCESS, prepareResponse(userRatings, userPredictions));
  }

  /**
   * Prepares a json sting of watched and recommended movies in the following format:
   * {"rated":["ratedMovie1","ratedMovie1"],"recommended":["recommendedMovie1","recommendedMovie2"]}
   * @param userRatings user given rating to movies
   * @param userPredictions movie recommendation to user with predicted rating
   * @return {@link JsonObject} of watched and recommended movies
   */
  private JsonObject prepareResponse(Iterator<KeyValue<byte[], UserScore>> userRatings,
                                     Iterator<KeyValue<byte[], Rating>> userPredictions) {

    JsonArray watched = new JsonArray();
    while (userRatings.hasNext()) {
      UserScore curRating = userRatings.next().getValue();
      watched.add(GSON.toJsonTree(movies.read(Bytes.toBytes(curRating.getMovieID()))));
    }

    JsonArray recommended = new JsonArray();
    while (userPredictions.hasNext()) {
      Rating curPrediction = userPredictions.next().getValue();
      recommended.add(GSON.toJsonTree(movies.read(Bytes.toBytes(curPrediction.product()))));
    }

    JsonObject response = new JsonObject();
    response.add("rated", watched);
    response.add("recommended", recommended);

    return response;
  }
}
