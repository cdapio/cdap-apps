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

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
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

    // Inject ratings data
    sendRatingsData();
    // Start the Spark Program
    SparkManager sparkManager = appManager.getSparkManager(RecommendationBuilder.class.getSimpleName()).start();
    sparkManager.waitForFinish(60, TimeUnit.SECONDS);

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
    ServiceManager serviceManager = appManager.getServiceManager(MovieRecommenderApp.RECOMMENDATION_SERVICE).start();
    serviceManager.waitForStatus(true);

    // Verify that recommendation are generated
    String response =
      doGet(new URL(serviceManager.getServiceURL(), MovieRecommenderServiceHandler.RECOMMEND + "/1"));

    Map<String, String[]> responseMap =
      GSON.fromJson(response, new TypeToken<Map<String, String[]>>() { }.getType());

    Assert.assertTrue(responseMap.containsKey("rated") && responseMap.get("rated").length > 0);
    Assert.assertTrue(responseMap.containsKey("recommended") && responseMap.get("recommended").length > 0);
  }

  private String doGet(URL url) throws IOException {
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    return response.getResponseBodyAsString();
  }

  /**
   * Sends movies data to {@link MovieRecommenderApp#DICTIONARY_SERVICE}
   */
  private void sendMovieData(ApplicationManager applicationManager) throws Exception {
    String moviesData = "0::Movie0\n1::Movie1\n2::Movie2\n3::Movie3\n";
    ServiceManager serviceManager =
      applicationManager.getServiceManager(MovieRecommenderApp.DICTIONARY_SERVICE).start();
    serviceManager.waitForStatus(true);

    URL url = new URL(serviceManager.getServiceURL(), MovieDictionaryServiceHandler.STORE_MOVIES);
    HttpRequest request = HttpRequest.post(url).withBody(moviesData, Charsets.UTF_8).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    LOG.debug("Sent movies data");
  }

  /**
   * Send some ratings to the Stream
   */
  private void sendRatingsData() throws IOException {
    StreamManager streamManager = getStreamManager(MovieRecommenderApp.RATINGS_STREAM);
    streamManager.send("0::0::3");
    streamManager.send("0::1::4");
    streamManager.send("1::1::4");
    streamManager.send("1::2::4");
    LOG.debug("Sent ratings data");
  }
}
