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

/**
 * Represents user's ratings for different movies
 */
public class UserScore implements Serializable {

  private static final long serialVersionUID = -1258765752695193629L;

  private final int userID;
  private final int movieID;
  private final int rating;

  public UserScore(int userID, int movieID, int rating) {
    this.userID = userID;
    this.movieID = movieID;
    this.rating = rating;
  }

  public int getUserID() {
    return userID;
  }

  public int getMovieID() {
    return movieID;
  }

  public int getRating() {
    return rating;
  }
}
