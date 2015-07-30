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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.Spark;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;

/**
 * A {@link Spark} program that creates a {@link MatrixFactorizationModel} from {@link UserScore} and recommend movies
 * to user by calculating {@link Rating} through {@link MatrixFactorizationModel#predict(RDD)}
 */
public class RecommendationBuilderSpecification extends AbstractSpark {
  @Override
  public void configure() {
    setName("RecommendationBuilder");
    setDescription("Spark program that computes movie recommendations.");
    setMainClass(RecommendationBuilder.class);
    setDriverResources(new Resources(1024));
    setExecutorResources(new Resources(1024));
  }
}
