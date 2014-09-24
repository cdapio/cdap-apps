package co.cask.cdap.examples.sentiment.web;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.ning.http.client.AsyncCompletionHandler;
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
 * Proxies POST requests to a given url (specified thru cdap.host and cdap.port system properties).
 * By default proxies to http://localhost:10000.
 * Needed for resolving cross-domain javascript complexities.
 */
public class ProxyServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyServlet.class);

  private String cdapURL;

  @Override
  public void init() throws ServletException {
    String host = System.getProperty("cdap.host");
    host = host == null ? "localhost" : host;
    String port = System.getProperty("cdap.port");
    port = port == null ? "10000" : port;
    cdapURL = String.format("http://%s:%s", host, port);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    AsyncHttpClient client =
      new AsyncHttpClient(new AsyncHttpClientConfig.Builder().setRequestTimeoutInMs(1000).build());
    try {
      String url = cdapURL + req.getPathInfo();
      byte[] bytes = ByteStreams.toByteArray(req.getInputStream());
      String responseBody;
      try {
        responseBody = client.preparePost(url).setBody(bytes).execute().get().getResponseBody();
      } catch (Exception e) {
        LOG.error("handling request failed", e);
        e.printStackTrace();
        throw Throwables.propagate(e);
      }

      PrintWriter out = resp.getWriter();
      out.write(responseBody);
      out.close();
    } finally {
      client.close();
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
    AsyncHttpClient client =
      new AsyncHttpClient(new AsyncHttpClientConfig.Builder().setRequestTimeoutInMs(1000).build());
    try {
      String url = cdapURL + req.getPathInfo() + "?" + req.getQueryString();
      try {
        client.prepareGet(url).execute(new AsyncCompletionHandler<Response>(){
          @Override
          public Response onCompleted(Response response) throws Exception {
            PrintWriter out = resp.getWriter();
            resp.setStatus(response.getStatusCode());
            try {
              out.write(response.getResponseBody());
            } finally {
              out.close();
            }
            return response;
          }
        }).get();

      } catch (Exception e) {
        LOG.error("handling request failed", e);
        e.printStackTrace();
        throw Throwables.propagate(e);
      }

    } finally {
      client.close();
    }
  }
}
