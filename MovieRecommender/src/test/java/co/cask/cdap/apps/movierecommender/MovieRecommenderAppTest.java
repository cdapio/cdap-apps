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


import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link MovieRecommenderApp}
 */
public class MovieRecommenderAppTest extends TestBase {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(MovieRecommenderAppTest.class);

  @Test
  public void testRecommendation() throws Exception {

    ApplicationManager appManager = deployApplication(MovieRecommenderApp.class);

    // Send movies data through service
    sendMovieData(appManager);

    try {
      // Inject ratings data
      sendRatingsData(appManager);
      // Start the Spark Program
      SparkManager sparkManager = appManager.startSpark(RecommendationBuilder.class.getSimpleName());
      sparkManager.waitForFinish(120, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.warn("Failed to send ratings data to {}", MovieRecommenderApp.RATINGS_STREAM, e);
      throw Throwables.propagate(e);
    }

    verifyRecommenderServiceHandler(appManager);
  }

  /**
   * Verify that the movie recommendations were generated for the users
   *
   * @param appManager {@link ApplicationManager} for the deployed application
   * @throws InterruptedException
   * @throws IOException
   */
  private void verifyRecommenderServiceHandler(ApplicationManager appManager) throws InterruptedException, IOException {
    ServiceManager serviceManager = appManager.startService(MovieRecommenderApp.RECOMMENDATION_SERVICE);
    serviceManager.waitForStatus(true);

    // Verify that recommendation are generated
    String response = requestService(new URL(serviceManager.getServiceURL(), MovieRecommenderServiceHandler.RECOMMEND +
      "/1"));

    Map<String, String[]> responseMap = GSON.fromJson(response, new TypeToken<Map<String, String[]>>() {
    }.getType());

    Assert.assertTrue(responseMap.containsKey("rated") && responseMap.get("rated").length > 0);
    Assert.assertTrue(responseMap.containsKey("recommended") && responseMap.get("recommended").length > 0);
  }

  private String requestService(URL url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    try {
      return new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Sends movies data to {@link MovieRecommenderApp#DICTIONARY_SERVICE}
   */
  private void sendMovieData(ApplicationManager applicationManager) {
    String moviesData = "0::Movie0\n1::Movie1\n2::Movie2\n3::Movie3\n";
    ServiceManager serviceManager = applicationManager.startService(MovieRecommenderApp.DICTIONARY_SERVICE);
    try {
      serviceManager.waitForStatus(true);
    } catch (InterruptedException e) {
      LOG.error("Failed to start {} service", MovieRecommenderApp.DICTIONARY_SERVICE, e);
      throw Throwables.propagate(e);
    }
    try {
      URL url = new URL(serviceManager.getServiceURL(), MovieDictionaryServiceHandler.STORE_MOVIES);
      HttpRequest request = HttpRequest.post(url).withBody(moviesData, Charsets.UTF_8).build();
      HttpResponse response = HttpRequests.execute(request);
      Assert.assertEquals(200, response.getResponseCode());
      LOG.debug("Sent movies data");
    } catch (IOException e) {
      LOG.warn("Failed to send movies data to {}", MovieRecommenderApp.DICTIONARY_SERVICE, e);
      throw Throwables.propagate(e);
    } finally {
      serviceManager.stop();
    }
  }

  /**
   * Send some ratings to the Stream
   */
  private void sendRatingsData(ApplicationManager appManager) throws IOException {
    StreamWriter streamWriter = appManager.getStreamWriter(MovieRecommenderApp.RATINGS_STREAM);
    streamWriter.send("0::0::3");
    streamWriter.send("0::1::4");
    streamWriter.send("1::1::4");
    streamWriter.send("1::2::4");
    LOG.debug("Sent ratings data");
  }
}
