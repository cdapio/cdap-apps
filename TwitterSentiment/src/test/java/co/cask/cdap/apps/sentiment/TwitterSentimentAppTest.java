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
package co.cask.cdap.apps.sentiment;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
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
    try {
      ApplicationManager appManager = deployApplication(TwitterSentimentApp.class);

      Map<String, String> args = Maps.newHashMap();
      args.put("disable.public", "true");

      // Starts a Flow
      FlowManager flowManager = appManager.startFlow(SentimentAnalysisFlow.FLOW_NAME, args);

      try {
        // Write a message to Stream
        StreamWriter streamWriter = appManager.getStreamWriter(TwitterSentimentApp.STREAM_NAME);
        streamWriter.send("i love movie");
        streamWriter.send("i hate movie");
        streamWriter.send("i am neutral to movie");
        streamWriter.send("i am happy today that I got this working.");

        // Wait for the last Flowlet processed all tokens
        RuntimeMetrics countMetrics = RuntimeStats.getFlowletMetrics(TwitterSentimentApp.NAME, SentimentAnalysisFlow.FLOW_NAME, CountSentimentFlowlet.NAME);
        countMetrics.waitForProcessed(4, 15, TimeUnit.SECONDS);
      } finally {
        flowManager.stop();
      }


      // Start service and verify
      ServiceManager serviceManager = appManager.startService(SentimentQueryService.SERVICE_NAME);
      serviceStatusCheck(serviceManager, true);
      URL url = new URL(serviceManager.getServiceURL(), "aggregates");

      try {
        // Verify the aggregates
        Map<String, Long> result = GSON.fromJson(doRequest(url), MAP_OF_STRING_LONG);
        Assert.assertEquals(2, result.get("positive").intValue());
        Assert.assertEquals(1, result.get("negative").intValue());
        Assert.assertEquals(1, result.get("neutral").intValue());
        // Verify retrieval of sentiments
        url = new URL(serviceManager.getServiceURL(), "sentiments/positive");
        result = GSON.fromJson(doRequest(url), MAP_OF_STRING_LONG);
        Assert.assertEquals(ImmutableSet.of("i love movie", "i am happy today that I got this working."),
                            result.keySet());
        url = new URL(serviceManager.getServiceURL(), "sentiments/negative?limit=10");
        result = GSON.fromJson(doRequest(url), MAP_OF_STRING_LONG);
        Assert.assertEquals(ImmutableSet.of("i hate movie"), result.keySet());

        url = new URL(serviceManager.getServiceURL(), "sentiments/neutral?seconds=300");
        result = GSON.fromJson(doRequest(url), MAP_OF_STRING_LONG);
        Assert.assertEquals(ImmutableSet.of("i am neutral to movie"), result.keySet());

        // Verify the counts of the following sentiments
        url = new URL(serviceManager.getServiceURL(), "counts");

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        String response;
        try {
          connection.getOutputStream().write(Bytes.toBytes("['positive','negative','neutral']"));
          response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
        } finally {
          connection.disconnect();
        }

        result = GSON.fromJson(response, MAP_OF_STRING_LONG);
        Assert.assertEquals(2, result.get("positive").intValue());
        Assert.assertEquals(1, result.get("negative").intValue());
        Assert.assertEquals(1, result.get("neutral").intValue());
      } finally {
        serviceManager.stop();
      }
    } finally {
      TimeUnit.SECONDS.sleep(1);
      RuntimeStats.clearStats("");
      clear();
    }
  }

  private void serviceStatusCheck(ServiceManager serviceManger, boolean running) throws InterruptedException {
    int trial = 0;
    while (trial++ < 5) {
      if (serviceManger.isRunning() == running) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new IllegalStateException("Service state not executed. Expected " + running);
  }

  private static String doRequest(URL url) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    String response;
    try {
      response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
    } finally {
      connection.disconnect();
    }
    return response;
  }

}
