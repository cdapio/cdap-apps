package co.cask.cdap.apps.netlens.app;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.apps.netlens.app.anomaly.AnomaliesServiceHandler;
import co.cask.cdap.apps.netlens.app.counter.DataPoint;
import co.cask.cdap.apps.netlens.app.counter.TopNTableUtil;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.io.Charsets;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NetlensAppTest extends TestBase {

  private static final Type DATA_POINT_LIST_TYPE = new TypeToken<List<DataPoint>>() {}.getType();
  private static final Type TOP_N_RESULT_LIST_TYPE = new TypeToken<List<TopNTableUtil.TopNResult>>() {}.getType();
  private static final long START = System.currentTimeMillis();
  private static final Gson GSON = new Gson();

  @Test
  public void testNetlensApp() throws Exception {
    try {
      ApplicationManager appManager = deployApplication(NetlensApp.class);

      Map<String, String> args = Maps.newHashMap();
      args.put("disable.public", "true");

      // Starts a Flow
      FlowManager flowManager = appManager.startFlow(AnalyticsFlow.NAME, args);

      try {
        // Write a message to Stream
        StreamWriter streamWriter = appManager.getStreamWriter(NetlensApp.STREAM_NAME);
        sendData(streamWriter);

        // Wait for the last Flowlet processed all tokens
        RuntimeMetrics countMetrics = RuntimeStats.getFlowletMetrics(NetlensApp.NAME, AnalyticsFlow.NAME,
                                                                     "traffic-count");
        countMetrics.waitForProcessed(1000, 60, TimeUnit.SECONDS);
      } finally {
        flowManager.stop();
      }

      ServiceManager anomaliesCountServiceManager = appManager.startService(NetlensApp.ANOMALIES_COUNT_SERVICE_NAME);
      ServiceManager anomaliesServiceManager = appManager.startService(NetlensApp.ANOMALIES_SERVICE_NAME);
      ServiceManager countersServiceManager = appManager.startService(NetlensApp.COUNTERS_SERVICE_NAME);

      anomaliesCountServiceManager.waitForStatus(true);
      anomaliesServiceManager.waitForStatus(true);
      countersServiceManager.waitForStatus(true);

      try {
        testAnomaliesCountService(anomaliesCountServiceManager);
        testAnomaliesService(anomaliesServiceManager);
        testCountersService(countersServiceManager);
      } finally {
        anomaliesCountServiceManager.stop();
        anomaliesServiceManager.stop();
        countersServiceManager.stop();

        anomaliesCountServiceManager.waitForStatus(false);
        anomaliesServiceManager.waitForStatus(false);
        countersServiceManager.waitForStatus(false);
      }
    } finally {
      TimeUnit.SECONDS.sleep(1);
      clear();
    }
  }

  private void testAnomaliesCountService(ServiceManager serviceManager) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), String.format("count/%d/%d",
                                                                    START, System.currentTimeMillis()));
    List<DataPoint> result = GSON.fromJson(doRequest(url), DATA_POINT_LIST_TYPE);
    Assert.assertFalse(result.isEmpty());

    url = new URL(serviceManager.getServiceURL(), String.format("uniqueIpsCount/%d/%d",
                                                                START, System.currentTimeMillis()));
    result = GSON.fromJson(doRequest(url), DATA_POINT_LIST_TYPE);
    Assert.assertFalse(result.isEmpty());

    url = new URL(serviceManager.getServiceURL(), String.format("topN/%d", START) + "?limit=20");
    List<TopNTableUtil.TopNResult> topNResults = GSON.fromJson(doRequest(url), TOP_N_RESULT_LIST_TYPE);
    Assert.assertFalse(topNResults.isEmpty());
  }

  private void testAnomaliesService(ServiceManager serviceManager) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), String.format("timeRange/%d/%d?groupFor=none",
                                                                    START, System.currentTimeMillis()));
    List<AnomaliesServiceHandler.Anomaly> result =
      GSON.fromJson(doRequest(url), new TypeToken<List<AnomaliesServiceHandler.Anomaly>>() {}.getType());
    Assert.assertFalse(result.isEmpty());
  }

  private void testCountersService(ServiceManager serviceManager) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), String.format("counts/%d/%d",
                                                                    START, System.currentTimeMillis()));
    List<DataPoint> result = GSON.fromJson(doRequest(url), DATA_POINT_LIST_TYPE);
    Assert.assertFalse(result.isEmpty());

    url = new URL(serviceManager.getServiceURL(), String.format("topN/%d", START) + "?limit=20");
    List<TopNTableUtil.TopNResult> topNResults = GSON.fromJson(doRequest(url), TOP_N_RESULT_LIST_TYPE);
    Assert.assertFalse(topNResults.isEmpty());
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

  private void sendData(StreamWriter streamWriter) throws IOException {
    File anomaliesData = new File(System.getProperty("user.dir").concat("/resources/anomalies.data"));
    Thread.currentThread().getContextClassLoader().getResource("anomalies.data");
    FileReader fileReader = new FileReader(anomaliesData);
    BufferedReader reader = new BufferedReader(fileReader);
    String line;
    while ((line = reader.readLine()) != null) {
      streamWriter.send(line);
    }
  }
}
