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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * A {@link Flowlet} that reads ratings from a {@link Stream} and saves them to a {@link Dataset}
 */
public class RatingWriter extends AbstractFlowlet {
  private static final Pattern FIELDS_DELIMITER = Pattern.compile("::");
  private static final Logger LOG = LoggerFactory.getLogger(RatingWriter.class);

  @UseDataSet("ratings")
  private ObjectStore<UserScore> ratings;

  @ProcessInput
  public void process(StreamEvent event) {
    String body = new String(event.getBody().array());
    LOG.trace("Ratings info: {}", body);

    String[] ratingData = FIELDS_DELIMITER.split(body.trim());
    UserScore userScore = new UserScore(Integer.parseInt(ratingData[0]), Integer.parseInt(ratingData[1]),
                                        Integer.parseInt(ratingData[2]));

    // key is userID+movieID
    byte[] key = Bytes.add(Bytes.toBytes(userScore.getUserID()), Bytes.toBytes(userScore.getMovieID()));
    ratings.write(key, userScore);
  }
}
