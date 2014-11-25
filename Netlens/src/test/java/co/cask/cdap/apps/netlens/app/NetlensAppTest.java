package co.cask.cdap.apps.netlens.app;

import co.cask.cdap.apps.netlens.app.anomaly.AnomaliesService;
import co.cask.cdap.apps.netlens.app.anomaly.AnomaliesServiceHandler;
import co.cask.cdap.apps.netlens.app.counter.AnomaliesCountService;
import co.cask.cdap.apps.netlens.app.counter.CountersService;
import co.cask.cdap.apps.netlens.app.counter.DataPoint;
import co.cask.cdap.apps.netlens.app.counter.TopNTableUtil;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeMetrics;
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

  private static final Type ANOMALY_LIST_TYPE = new TypeToken<List<AnomaliesServiceHandler.Anomaly>>() {}.getType();
  private static final Type DATA_POINT_ARRAY_TYPE = new TypeToken<DataPoint[]>() {}.getType();
  private static final Type TOP_N_RESULT_LIST_TYPE = new TypeToken<List<TopNTableUtil.TopNResult>>() {}.getType();
  private static final long START = System.currentTimeMillis();

  @Test
  public void testSentimentProcedure() throws Exception {
    try {
      ApplicationManager appManager = deployApplication(NetlensApp.class);

      Map<String, String> args = Maps.newHashMap();
      args.put("disable.public", "true");

      // Starts a Flow
      FlowManager flowManager = appManager.startFlow(AnalyticsFlow.FLOW_NAME, args);

      try {
        // Write a message to Stream
        StreamWriter streamWriter = appManager.getStreamWriter(NetlensApp.STREAM_NAME);
        sendData(streamWriter);

        // Wait for the last Flowlet processed all tokens
        RuntimeMetrics countMetrics = RuntimeStats.getFlowletMetrics(NetlensApp.APP_NAME, AnalyticsFlow.FLOW_NAME, "traffic-count");
        countMetrics.waitForProcessed(1000, 60, TimeUnit.SECONDS);
      } finally {
        flowManager.stop();
      }

      testAnomaliesCountService(appManager);
      testAnomaliesService(appManager);
      testCountersService(appManager);

    } finally {
      TimeUnit.SECONDS.sleep(1);
      RuntimeStats.clearStats("");
      clear();
    }
  }

  private void testAnomaliesCountService(ApplicationManager appManager) throws Exception {
    ServiceManager serviceManager = appManager.startService(AnomaliesCountService.SERVICE_NAME);
    serviceStatusCheck(serviceManager, true);
    URL url = new URL(String.format("%scount/%d/%d", serviceManager.getServiceURL(),
                                    START, System.currentTimeMillis()));
    try {
      DataPoint[] result = new Gson().fromJson(doRequest(url), DATA_POINT_ARRAY_TYPE);
      Assert.assertFalse(result.length == 0);

      url = new URL(String.format("%suniqueIpsCount/%d/%d", serviceManager.getServiceURL(),
                                  START, System.currentTimeMillis()));
      result = new Gson().fromJson(doRequest(url), DATA_POINT_ARRAY_TYPE);
      Assert.assertFalse(result.length == 0);

      url = new URL(String.format("%stopN/%d", serviceManager.getServiceURL(), START));
      List<TopNTableUtil.TopNResult> topNResults = new Gson().fromJson(doRequest(url), TOP_N_RESULT_LIST_TYPE);
      Assert.assertFalse(topNResults.isEmpty());
    } finally {
      serviceManager.stop();
    }
  }

  private void testAnomaliesService(ApplicationManager appManager) throws Exception {
    ServiceManager serviceManager = appManager.startService(AnomaliesService.SERVICE_NAME);
    serviceStatusCheck(serviceManager, true);
    URL url = new URL(String.format("%stimeRange/%d/%d?groupFor=none", serviceManager.getServiceURL(),
                                    START, System.currentTimeMillis()));
    try {
      List<AnomaliesServiceHandler.Anomaly> result = new Gson().fromJson(doRequest(url), ANOMALY_LIST_TYPE);
      Assert.assertFalse(result.isEmpty());
    } finally {
      serviceManager.stop();
    }
  }

  private void testCountersService(ApplicationManager appManager) throws Exception {
    ServiceManager serviceManager = appManager.startService(CountersService.SERVICE_NAME);
    serviceStatusCheck(serviceManager, true);
    URL url = new URL(String.format("%scounts/%d/%d", serviceManager.getServiceURL(),
                                    START, System.currentTimeMillis()));
    try {
      DataPoint[] result = new Gson().fromJson(doRequest(url), DATA_POINT_ARRAY_TYPE);
      Assert.assertFalse(result.length == 0);

      url = new URL(String.format("%stopN/%d", serviceManager.getServiceURL(), START));
      List<TopNTableUtil.TopNResult> topNResults = new Gson().fromJson(doRequest(url), TOP_N_RESULT_LIST_TYPE);
      Assert.assertFalse(topNResults.isEmpty());
    } finally {
      serviceManager.stop();
    }
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
