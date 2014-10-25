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

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TwitterSentimentAppTest extends TestBase {

  @Test
  public void testSentimentProcedure() throws Exception {
    ApplicationManager appManager = deployApplication(TwitterSentimentApp.class);

    Map<String, String> args = Maps.newHashMap();
    args.put("disable.public", "true");

    // Starts a Flow
    FlowManager flowManager = appManager.startFlow(SentimentAnalysisFlow.FLOW_NAME, args);

    // Write a message to Stream
    StreamWriter streamWriter = appManager.getStreamWriter(TwitterSentimentApp.STREAM_NAME);
    streamWriter.send("i love movie");
    streamWriter.send("i hate movie");
    streamWriter.send("i am neutral to movie");
    streamWriter.send("i am happy today that I got this working.");

    // Wait for the last Flowlet processed all tokens
    RuntimeMetrics countMetrics = RuntimeStats.getFlowletMetrics(TwitterSentimentApp.NAME, SentimentAnalysisFlow.FLOW_NAME, CountSentimentFlowlet.NAME);
    countMetrics.waitForProcessed(4, 15, TimeUnit.SECONDS);

    // Start procedure and verify
    ProcedureManager procedureManager = appManager.startProcedure(SentimentQueryProcedure.PROCEDURE_NAME);
    String response = procedureManager.getClient().query("aggregates", Collections.<String, String>emptyMap());

    // Verify the aggregates
    Map<String, Long> result = new Gson().fromJson(response, new TypeToken<Map<String, Long>>() { }.getType());
    Assert.assertEquals(2, result.get("positive").intValue());
    Assert.assertEquals(1, result.get("negative").intValue());
    Assert.assertEquals(1, result.get("neutral").intValue());

    // Verify retrieval of sentiments
    response = procedureManager.getClient().query("sentiments", ImmutableMap.of("sentiment", "positive"));
    result = new Gson().fromJson(response, new TypeToken<Map<String, Long>>() { }.getType());
    Assert.assertEquals(ImmutableSet.of("i love movie", "i am happy today that I got this working."),
                        result.keySet());

    response = procedureManager.getClient().query("sentiments", ImmutableMap.of("sentiment", "negative"));
    result = new Gson().fromJson(response, new TypeToken<Map<String, Long>>() { }.getType());
    Assert.assertEquals(ImmutableSet.of("i hate movie"), result.keySet());

    response = procedureManager.getClient().query("sentiments", ImmutableMap.of("sentiment", "neutral"));
    result = new Gson().fromJson(response, new TypeToken<Map<String, Long>>() { }.getType());
    Assert.assertEquals(ImmutableSet.of("i am neutral to movie"), result.keySet());


    // Verify the counts of the following sentiments
    Map<String, String> sentiments = Maps.newHashMap();
    sentiments.put("sentiments", "['positive','negative','neutral']");
    response = procedureManager.getClient().query("counts", sentiments);

    result = new Gson().fromJson(response, new TypeToken<Map<String, Long>>() { }.getType());
    Assert.assertEquals(2, result.get("positive").intValue());
    Assert.assertEquals(1, result.get("negative").intValue());
    Assert.assertEquals(1, result.get("neutral").intValue());
  }

}
