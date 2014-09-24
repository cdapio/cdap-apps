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
package co.cask.cdap.apps.wise;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.apps.wise.BounceCountStore;
import co.cask.cdap.apps.wise.PageBounce;
import co.cask.cdap.apps.wise.WiseApp;
import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpRequests;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests of the Wise application.
 */
public class WiseAppTest extends TestBase {
  @Test
  public void test() throws Exception {
    // Deploy an App
    ApplicationManager appManager = deployApplication(WiseApp.class);

    // Start a Flow
    FlowManager flowManager = appManager.startFlow("WiseFlow");

    try {
      sendData(appManager);

      // Wait for the last Flowlet to process 5 events
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("Wise", "WiseFlow", "pageViewCount");
      metrics.waitForProcessed(5, 5, TimeUnit.SECONDS);

      // Verify the processed data
      checkPageViewService(appManager);
    } finally {
      flowManager.stop();
    }

    // Test the MapReduce job
    MapReduceManager mrManager = appManager.startMapReduce("WiseWorkflow_BounceCountsMapReduce");
    mrManager.waitForFinish(3, TimeUnit.MINUTES);

    // Check the data outputted from the MapReduce job
    DataSetManager<BounceCountStore> dsManager = appManager.getDataSet("bounceCountStore");
    BounceCountStore bounceCountStore = dsManager.get();
    Assert.assertEquals(new PageBounce("/product.html", 3, 2), bounceCountStore.get("/product.html"));
    Assert.assertEquals(new PageBounce("/career.html", 2, 1), bounceCountStore.get("/career.html"));

    // Perform a SQL query on the bounceCounts dataset to retrieve the same results
    Connection exploreConnection = getQueryClient();
    ResultSet resultSet =
      exploreConnection.prepareStatement("SELECT * FROM cdap_user_bouncecountstore ORDER BY uri").executeQuery();
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals("/career.html", resultSet.getString(1));
    Assert.assertEquals(2L, resultSet.getLong(2));
    Assert.assertEquals(1L, resultSet.getLong(3));
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals("/product.html", resultSet.getString(1));
    Assert.assertEquals(3L, resultSet.getLong(2));
    Assert.assertEquals(2L, resultSet.getLong(3));
  }

  /**
   * Send a few events to the Stream.
   *
   * @param appManager an ApplicationManger instance.
   * @throws java.io.IOException
   */
  private void sendData(ApplicationManager appManager)
    throws IOException {
    // Define a StreamWriter to send Apache log events in String format to the App
    StreamWriter streamWriter = appManager.getStreamWriter("logEventStream");

    streamWriter.send("1.202.218.8 - - [12/Apr/2012:02:03:43 -0400] " +
                        "\"GET /product.html HTTP/1.0\" 404 208 \"http://www.example.org\" \"Mozilla/5.0\"");
    streamWriter.send("124.115.0.140 - - [12/Apr/2012:02:28:49 -0400] " +
                        "\"GET /product.html HTTP/1.1\" 200 392 \"http://www.example.org\" " +
                        "\"Sosospider+(+http://help.soso.com/webspider.htm)\"");
    streamWriter.send("83.160.166.85 - - [12/Apr/2012:22:59:12 -0400] " +
                        "\"GET /career.html HTTP/1.1\" 404 208 \"http://www.example.org\" \"portscout/0.8.1\"");
    streamWriter.send("1.202.218.8 - - [12/Apr/2012:02:11:43 -0400] " +
                        "\"GET /career.html HTTP/1.0\" 404 208 \"http://www.example.org\" \"Mozilla/5.0\"");
    streamWriter.send("1.202.218.8 - - [12/Apr/2012:02:13:43 -0400] " +
                        "\"GET /product.html HTTP/1.0\" 404 208 \"http://www.example.org\" \"Mozilla/5.0\"");
  }

  /**
   * Checks that log events were processed by WiseFlow and stored into the pageViewStore Dataset
   * using the WiseService.
   *
   * @param appManager an ApplicationManger instance.
   * @throws Exception
   */
  private void checkPageViewService(ApplicationManager appManager) throws Exception {

    ServiceManager serviceManager = appManager.startService("WiseService");
    serviceStatusCheck(serviceManager, true);

    URL url = new URL(serviceManager.getServiceURL(), "ip/1.202.218.8/count");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("3", Bytes.toString(response.getResponseBody()));

    url = new URL(serviceManager.getServiceURL(), "ip/1.202.218.8/uri/%252Fcareer.html/count");
    request = HttpRequest.get(url).build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals("1", Bytes.toString(response.getResponseBody()));

    serviceManager.stop();
    serviceStatusCheck(serviceManager, false);
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
}
