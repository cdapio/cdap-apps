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

package co.cask.cdap.apps.netlens.app;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.apps.netlens.app.anomaly.AnomaliesServiceHandler;
import co.cask.cdap.apps.netlens.app.counter.DataPoint;
import co.cask.cdap.apps.netlens.app.counter.TopNTableUtil;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class NetlensAppTest extends TestBase {

  private static final Type DATA_POINT_LIST_TYPE = new TypeToken<List<DataPoint>>() {}.getType();
  private static final Type TOP_N_RESULT_LIST_TYPE = new TypeToken<List<TopNTableUtil.TopNResult>>() {}.getType();
  private static final long START = System.currentTimeMillis();
  private static final Gson GSON = new Gson();

  @Test
  public void testNetlensApp() throws Exception {
    ApplicationManager appManager = deployApplication(NetlensApp.class);

    // Starts a Flow
    FlowManager flowManager = appManager.getFlowManager(AnalyticsFlow.NAME).start();

    // Write a message to Stream
    StreamManager streamWriter = getStreamManager(NetlensApp.STREAM_NAME);
    sendData(streamWriter);

    // Wait for the last Flowlet processed all tokens
    RuntimeMetrics countMetrics = flowManager.getFlowletMetrics("traffic-count");
    countMetrics.waitForProcessed(1000, 60, TimeUnit.SECONDS);

    ServiceManager anomaliesCountServiceManager =
      appManager.getServiceManager(NetlensApp.ANOMALIES_COUNT_SERVICE_NAME).start();
    ServiceManager anomaliesServiceManager = appManager.getServiceManager(NetlensApp.ANOMALIES_SERVICE_NAME).start();
    ServiceManager countersServiceManager = appManager.getServiceManager(NetlensApp.COUNTERS_SERVICE_NAME).start();

    anomaliesCountServiceManager.waitForStatus(true);
    anomaliesServiceManager.waitForStatus(true);
    countersServiceManager.waitForStatus(true);

    testAnomaliesCountService(anomaliesCountServiceManager);
    testAnomaliesService(anomaliesServiceManager);
    testCountersService(countersServiceManager);
  }

  private void testAnomaliesCountService(ServiceManager serviceManager) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), String.format("count/%d/%d",
                                                                    START, System.currentTimeMillis()));
    List<DataPoint> result = GSON.fromJson(doGet(url), DATA_POINT_LIST_TYPE);
    Assert.assertFalse(result.isEmpty());

    url = new URL(serviceManager.getServiceURL(), String.format("uniqueIpsCount/%d/%d",
                                                                START, System.currentTimeMillis()));
    result = GSON.fromJson(doGet(url), DATA_POINT_LIST_TYPE);
    Assert.assertFalse(result.isEmpty());

    url = new URL(serviceManager.getServiceURL(), String.format("topN/%d", START) + "?limit=20");
    List<TopNTableUtil.TopNResult> topNResults = GSON.fromJson(doGet(url), TOP_N_RESULT_LIST_TYPE);
    Assert.assertFalse(topNResults.isEmpty());
  }

  private void testAnomaliesService(ServiceManager serviceManager) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), String.format("timeRange/%d/%d?groupFor=none",
                                                                    START, System.currentTimeMillis()));
    List<AnomaliesServiceHandler.Anomaly> result =
      GSON.fromJson(doGet(url), new TypeToken<List<AnomaliesServiceHandler.Anomaly>>() {}.getType());
    Assert.assertFalse(result.isEmpty());
  }

  private void testCountersService(ServiceManager serviceManager) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), String.format("counts/%d/%d",
                                                                    START, System.currentTimeMillis()));
    List<DataPoint> result = GSON.fromJson(doGet(url), DATA_POINT_LIST_TYPE);
    Assert.assertFalse(result.isEmpty());

    url = new URL(serviceManager.getServiceURL(), String.format("topN/%d", START) + "?limit=20");
    List<TopNTableUtil.TopNResult> topNResults = GSON.fromJson(doGet(url), TOP_N_RESULT_LIST_TYPE);
    Assert.assertFalse(topNResults.isEmpty());
  }

  private static String doGet(URL url) throws IOException {
    return HttpRequests.execute(HttpRequest.get(url).build()).getResponseBodyAsString();
  }

  private void sendData(StreamManager streamManager) throws IOException {
    File anomaliesData = new File(System.getProperty("user.dir").concat("/resources/anomalies.data"));
    Thread.currentThread().getContextClassLoader().getResource("anomalies.data");
    FileReader fileReader = new FileReader(anomaliesData);
    BufferedReader reader = new BufferedReader(fileReader);
    String line;
    while ((line = reader.readLine()) != null) {
      streamManager.send(line);
    }
  }
}
