/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectStores;

/**
 * Application that provides movie recommendations to users.
 */
public class MovieRecommenderApp extends AbstractApplication {

  public static final String RECOMMENDATION_SERVICE = "MovieRecommenderService";
  public static final String DICTIONARY_SERVICE = "MovieDictionaryService";
  public static final String RATINGS_STREAM = "ratingsStream";

  @Override
  public void configure() {
    setName("MovieRecommender");
    setDescription("Movie Recommendation App");
    addStream(new Stream(RATINGS_STREAM));
    addSpark(new RecommendationBuilderSpecification());
    addService(RECOMMENDATION_SERVICE, new MovieRecommenderServiceHandler());
    addService(DICTIONARY_SERVICE, new MovieDictionaryServiceHandler());

    try {
      ObjectStores.createObjectStore(getConfigurer(), "ratings", UserScore.class);
      ObjectStores.createObjectStore(getConfigurer(), "recommendations", UserScore.class);
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
