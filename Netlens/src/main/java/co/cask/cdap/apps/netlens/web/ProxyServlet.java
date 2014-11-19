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

package co.cask.cdap.apps.netlens.web;

import com.google.common.base.Throwables;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Proxies POST requests to a given url (specified thru <code>cdap.host</code> and
 * <code>cdap.port</code> system properties); by default, proxies to <code>http://localhost:10000</code>.
 * <p>
 * Needed for resolving cross-domain javascript complexities.
 * </p>
 */
public class ProxyServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyServlet.class);
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "10000";

  private String cdapURL;

  @Override
  public void init() throws ServletException {
    String host = System.getProperty("cdap.host", DEFAULT_HOST);
    String port = System.getProperty("cdap.port", DEFAULT_PORT);
    cdapURL = String.format("http://%s:%s", host, port);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    AsyncHttpClient client =
      new AsyncHttpClient(new AsyncHttpClientConfig.Builder().setRequestTimeoutInMs(1000).build());
    try {
      String url = cdapURL + req.getPathInfo();
      String responseBody;
      try {
        Response serviceResponse = client.prepareGet(url).execute().get();
        responseBody = serviceResponse.getResponseBody();
      } catch (Exception e) {
        LOG.error("handling request failed", e);
        e.printStackTrace();
        throw Throwables.propagate(e);
      }

      PrintWriter out = resp.getWriter();
      resp.setContentType("application/json");
      out.write(responseBody);
      out.close();
    } finally {
      client.close();
    }
  }
}
