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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Service that exposes endpoints to retrieve page view counts.
 */
class WiseService extends AbstractService {
  @Override
  protected void configure() {
    setName("WiseService");
    addHandler(new PageViewCountHandler());
  }

  /**
   * Handler which defines HTTP endpoints to access information stored in the {@code pageViewStore} Dataset.
   */
  public static class PageViewCountHandler extends AbstractHttpServiceHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PageViewCountHandler.class);

    // Annotation indicates that the pageViewStore custom DataSet is used in the Service
    @UseDataSet("pageViewStore")
    private PageViewStore pageViewStore;

    /**
     * Queries the total number of visited pages viewed from a specified IP address. It expects GET request to
     * a URL Of the form:
     *
     * <pre>{@code
     *
     * GET http://[host]:[port]/v2/apps/Wise/services/WiseService/methods/ip/[ip]/count
     * }</pre>
     */
    @GET
    @Path("/ip/{ip}/count")
    public void getIPCount(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("ip") String ipAddress) {
      long counts = pageViewStore.getCounts(ipAddress);
      responder.sendJson(200, counts);
    }

    /**
     * Queries the page view count of a given URI of an IP. It expects POST request to a URL of the form:
     *
     * <pre>{@code
     *
     * POST http://[host]:[port]/v2/apps/Wise/services/WiseService/methods/ip/[ip]/count
     * }</pre>
     *
     * With the URI to query form in the POST body.
     */
    @POST
    @Path("/ip/{ip}/count")
    public void getPageViewCount(HttpServiceRequest request, HttpServiceResponder responder,
                                 @PathParam("ip") String ipAddress) {
      // The whole request body is the request URI.
      String uri = Charsets.UTF_8.decode(request.getContent()).toString();
      Map<String, Long> pageCounts = pageViewStore.getPageCount(ipAddress);
      Long counts = pageCounts.get(uri);
      if (counts == null) {
        LOG.debug("No entry found for page URI: {} and IP address: {}", uri, ipAddress);
        counts = 0L;
      }
      responder.sendJson(200, counts);
    }
  }
}
