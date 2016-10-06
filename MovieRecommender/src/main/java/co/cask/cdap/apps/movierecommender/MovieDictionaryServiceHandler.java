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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.regex.Pattern;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that exposes HTTP API to update movie dictionary
 */
public class MovieDictionaryServiceHandler extends AbstractHttpServiceHandler {
  private static final Pattern NEWLINE_DELIMITER = Pattern.compile("[\\r\\n]+");
  private static final Pattern FIELDS_DELIMITER = Pattern.compile("::");
  public static final String STORE_MOVIES = "/v1/movies";
  private static final String GET_MOVIE = "/v1/movies/{movie-id}";

  @UseDataSet("movies")
  private ObjectStore<String> movies;

  @Path(STORE_MOVIES)
  @POST
  public void uploadHandler(HttpServiceRequest request, HttpServiceResponder responder) {
    ByteBuffer requestContents = request.getContent();
    if (requestContents == null) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Movies information is empty.");
      return;
    }

    String moviesData = Charsets.UTF_8.decode(requestContents).toString();

    if (parseAndStoreMovies(moviesData)) {
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    } else {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Malformed movies information.");
    }
  }

  @Path(GET_MOVIE)
  @GET
  public void getMovie(HttpServiceRequest request,
                       HttpServiceResponder responder,
                       @PathParam("movie-id") int movieId) {
    String movie = movies.read(Bytes.toBytes(movieId));
    if (Strings.isNullOrEmpty(movie)) {
      responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, String.format("No movie with id %s found.", movieId));
    } else {
      responder.sendString(HttpURLConnection.HTTP_OK, movie, Charset.defaultCharset());
    }
  }

  private boolean parseAndStoreMovies(String moviesData) {
    String[] lines = NEWLINE_DELIMITER.split(moviesData.trim());

    for (String movie : lines) {
      String[] movieInfo = FIELDS_DELIMITER.split(movie.trim());
      if (!movieInfo[0].isEmpty() && !movieInfo[1].isEmpty()) {
        movies.write(Bytes.toBytes(Integer.parseInt(movieInfo[0])), movieInfo[1]);
      } else {
        return false;
      }
    }
    return true;
  }
}
