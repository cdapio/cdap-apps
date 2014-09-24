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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.nio.ByteBuffer;
import java.util.regex.Pattern;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Class which handles the movies file submission by parsing and storing it in a {@link Dataset}
 */
public class MovieDictionaryServiceHandler extends AbstractHttpServiceHandler {

  private static final Pattern NEWLINE_DELIMITER = Pattern.compile("[\\r\\n]+");

  @UseDataSet("movies")
  private ObjectStore<String> moviesStore;

  @Path("storemovies")
  @POST
  public void uploadHandler(HttpServiceRequest request, HttpServiceResponder responder) {

    ByteBuffer requestContents = request.getContent();
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.NO_CONTENT.code(), "Movies information is empty.");
      return;
    }

    String moviesData = MovieSteerApp.CHARSET_UTF8.decode(requestContents).toString();

    if (parseAndStoreMovies(moviesData)) {
      responder.sendStatus(HttpResponseStatus.OK.code());
    } else {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.code(), "Malformed movies information.");
    }
  }

  private boolean parseAndStoreMovies(String moviesData) {
    boolean validRequest = false;
    String[] movies = NEWLINE_DELIMITER.split(moviesData.trim());

    for (String movie : movies) {
      String[] movieInfo = MovieSteerApp.RAW_DATA_DELIMITER.split(movie.trim());
      if (!movieInfo[0].isEmpty() && !movieInfo[1].isEmpty()) {
        validRequest = true;
        moviesStore.write(Bytes.toBytes(Integer.parseInt(movieInfo[0])), movieInfo[1]);
      }
    }
    return validRequest;
  }
}
