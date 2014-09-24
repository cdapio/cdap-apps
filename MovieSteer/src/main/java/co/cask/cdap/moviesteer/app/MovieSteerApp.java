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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.internal.io.UnsupportedTypeException;
import org.apache.spark.mllib.recommendation.Rating;

import java.nio.charset.Charset;
import java.util.regex.Pattern;

/**
 * MoveSteer: Movie Recommendation App based on Spark's MLibs using MovieLens dataset
 */
public class MovieSteerApp extends AbstractApplication {

  static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");
  static final Pattern RAW_DATA_DELIMITER = Pattern.compile("::");

  @Override
  public void configure() {
    setName("MovieSteer");
    setDescription("Movie Recommendation App");
    addStream(new Stream("ratingsStream"));
    addFlow(new RatingsFlow());
    addSpark(new MovieSteerSparkSpecification());
    addProcedure(new PredictionProcedure());
    addService(new MovieDictionaryService());

    try {
      ObjectStores.createObjectStore(getConfigurer(), "ratings", UserScore.class);
      ObjectStores.createObjectStore(getConfigurer(), "predictions", Rating.class);
      ObjectStores.createObjectStore(getConfigurer(), "movies", String.class);
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectStore if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because String is an actual class.
      throw new RuntimeException(e);
    }
  }

}
