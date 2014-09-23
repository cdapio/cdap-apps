package co.cask.cdap.examples.sentiment.web;

import co.cask.cdap.api.common.Bytes;
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

  @Override
  protected void doGet(HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
    AsyncHttpClient client =
      new AsyncHttpClient(new AsyncHttpClientConfig.Builder().setRequestTimeoutInMs(1000).build());
    try {
      String url = "http://localhost:10000" + req.getPathInfo() + "?" + req.getQueryString();
//http://localhost:8080/SentimentAnalysis/proxy/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments?sentiment=positive&_=1411427545765
//      System.out.println(req.getPathInfo());
//      /v2/apps/sentiment/procedures/sentiment-query/methods/sentiments
//      System.out.println(req.getRequestURL());
//      http://localhost:8080/SentimentAnalysis/proxy/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments
//      System.out.println(req.getRequestURI());
//      /SentimentAnalysis/proxy/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments
//      System.out.println(req.getQueryString());
//      sentiment=positive&_=1411427545765
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
