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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A class to collect rated and recommended movies.
 */
public class Recommendation implements Serializable {
  private static final long serialVersionUID = -1258787639695193629L;

  private List<MovieRating> rated = new ArrayList();
  private List<String> recommended = new ArrayList();

  public List<MovieRating> getRated() {
    return rated;
  }

  public List<String> getRecommended() {
    return recommended;
  }

  public void addRated(String movie, double rating) {
    rated.add(new MovieRating(movie, rating));
  }

  public void addRecommended(String movie) {
    recommended.add(movie);
  }

  private static class MovieRating {
    private String movie;
    private double rating;

    public String getMovie() {
      return movie;
    }

    public double getRating() {
      return rating;
    }

    public MovieRating(String movie, double rating) {
      this.movie = movie;
      this.rating = rating;
    }
  }
}
