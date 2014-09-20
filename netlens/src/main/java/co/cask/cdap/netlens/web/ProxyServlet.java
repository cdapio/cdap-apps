package co.cask.cdap.netlens.web;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Proxies POST requests to http://localhost:10000.
 *
 * Needed for resolving cross-domain javascript complexities.
 */
public class ProxyServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyServlet.class);

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    AsyncHttpClient client =
      new AsyncHttpClient(new AsyncHttpClientConfig.Builder().setRequestTimeoutInMs(1000).build());
    try {
      String url = "http://localhost:10000" + req.getPathInfo();
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
}
