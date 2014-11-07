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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.spark.mllib.recommendation.Rating;

import java.util.Iterator;

/**
 * Helper class, which contains common stuff.
 */
public class MovieRecommenderHelper {
  private static final Gson GSON = new Gson();

  /**
   * Prepares a json sting of watched and recommended movies in the following format:
   * {"rated":["ratedMovie1","ratedMovie2"],"recommended":["recommendedMovie1","recommendedMovie2"]}
   *
   * @param userRatings user given rating to movies
   * @param userPredictions movie recommendation to user with predicted rating
   *
   * @return {@link com.google.gson.JsonObject} of watched and recommended movies
   */
  public static JsonObject prepareResponse(ObjectStore<String> store,
                                           Iterator<KeyValue<byte[], UserScore>> userRatings,
                                           Iterator<KeyValue<byte[], Rating>> userPredictions) {
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
