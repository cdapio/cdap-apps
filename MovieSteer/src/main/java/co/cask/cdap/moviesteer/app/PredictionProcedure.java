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

package co.cask.cdap.moviesteer.app;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import org.apache.spark.mllib.recommendation.Rating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Procedure that returns movie recommendations for users
 */
public class PredictionProcedure extends AbstractProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(PredictionProcedure.class);

  @UseDataSet("predictions")
  private ObjectStore<Rating> predictions;

  @UseDataSet("ratings")
  private ObjectStore<UserScore> ratings;

  @Handle("getPrediction")
  public void getPrediction(ProcedureRequest request, ProcedureResponder responder)
    throws IOException, InterruptedException {

    String userId = request.getArgument("userId");
    if (userId == null) {
      responder.error(ProcedureResponse.Code.CLIENT_ERROR, "userId must be given as argument");
      return;
    }
    byte[] userID = ("" + userId).getBytes();

    //TODO: This scan with partial key scans the whole table. Figure out how to do it properly if it can be done
    Iterator<KeyValue<byte[], Rating>> userPredictions = predictions.scan(userID, null);
    Iterator<KeyValue<byte[], UserScore>> userRatings = ratings.scan(userID, null);

    if (userPredictions == null || userRatings == null) {
      responder.error(ProcedureResponse.Code.NOT_FOUND,
                      String.format("No ratings/prediction found for user %s", userId));
      return;
    }

    while (userRatings.hasNext()) {
      UserScore curRating = userRatings.next().getValue();
      System.out.println("User: " + curRating.getUserID() + " Movie: " + curRating.getMovieID() + " Rating: "
                           + curRating.getRating());
    }
    while (userPredictions.hasNext()) {
      Rating curPrediction = userPredictions.next().getValue();
      System.out.println("User: " + curPrediction.user() + " Movie: " + curPrediction.product() + " Rating: "
                           + curPrediction.rating());
    }

    //TODO: Send the rating and prediction objects as json
    responder.sendJson(ProcedureResponse.Code.SUCCESS, "Yo! Things worked");
  }
}
