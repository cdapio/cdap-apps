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

package co.cask.cdap.apps.sentiment;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TwitterSentimentAppTest extends TestBase {

  private static final Gson GSON = new Gson();
  private static final Type MAP_OF_STRING_LONG = new TypeToken<Map<String, Long>>() {}.getType();

  @Test
  public void testSentimentService() throws Exception {
    ApplicationManager appManager = deployApplication(TwitterSentimentApp.class);

    // Starts a Flow
    FlowManager flowManager =
      appManager.getFlowManager(SentimentAnalysisFlow.FLOW_NAME).start(ImmutableMap.of("disable.public", "true"));

    // Write a message to Stream
    StreamManager streamWriter = getStreamManager(TwitterSentimentApp.STREAM_NAME);
    streamWriter.send("i love movie");
    streamWriter.send("i hate movie");
    streamWriter.send("i am neutral to movie");
    streamWriter.send("i am happy today that I got this working.");

    // Wait for the last Flowlet processed all tokens

    RuntimeMetrics countMetrics = flowManager.getFlowletMetrics(CountSentimentFlowlet.NAME);
    countMetrics.waitForProcessed(4, 15, TimeUnit.SECONDS);

    // Start service and verify
    ServiceManager serviceManager = appManager.getServiceManager(SentimentQueryService.SERVICE_NAME).start();
    serviceManager.waitForStatus(true);

    URL url = new URL(serviceManager.getServiceURL(), "aggregates");

    // Verify the aggregates
    Map<String, Long> result = GSON.fromJson(doGet(url), MAP_OF_STRING_LONG);
    Assert.assertEquals(2, result.get("positive").intValue());
    Assert.assertEquals(1, result.get("negative").intValue());
    Assert.assertEquals(1, result.get("neutral").intValue());
    // Verify retrieval of sentiments
    url = new URL(serviceManager.getServiceURL(), "sentiments/positive");
    result = GSON.fromJson(doGet(url), MAP_OF_STRING_LONG);
    Assert.assertEquals(ImmutableSet.of("i love movie", "i am happy today that I got this working."),
                        result.keySet());
    url = new URL(serviceManager.getServiceURL(), "sentiments/negative?limit=10");
    result = GSON.fromJson(doGet(url), MAP_OF_STRING_LONG);
    Assert.assertEquals(ImmutableSet.of("i hate movie"), result.keySet());

    url = new URL(serviceManager.getServiceURL(), "sentiments/neutral?seconds=300");
    result = GSON.fromJson(doGet(url), MAP_OF_STRING_LONG);
    Assert.assertEquals(ImmutableSet.of("i am neutral to movie"), result.keySet());

    // Verify the counts of the following sentiments
    url = new URL(serviceManager.getServiceURL(), "counts");

    HttpResponse response =
      HttpRequests.execute(HttpRequest.post(url).withBody("['positive','negative','neutral']").build());

    result = GSON.fromJson(response.getResponseBodyAsString(), MAP_OF_STRING_LONG);
    Assert.assertEquals(2, result.get("positive").intValue());
    Assert.assertEquals(1, result.get("negative").intValue());
    Assert.assertEquals(1, result.get("neutral").intValue());
  }

  private static String doGet(URL url) throws IOException {
    return HttpRequests.execute(HttpRequest.get(url).build()).getResponseBodyAsString();
  }
}
