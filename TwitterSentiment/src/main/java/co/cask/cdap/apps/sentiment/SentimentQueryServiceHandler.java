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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Handler that exposes HTTP endpoints to retrieve the aggregates timeseries sentiment data.
 */
public class SentimentQueryServiceHandler extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SentimentQueryServiceHandler.class);
  private static final Gson GSON = new Gson();
  private static final String DEFAULT_LIMIT = "10";


  @UseDataSet(TwitterSentimentApp.TABLE_NAME)
  private Table sentiments;

  @UseDataSet(TwitterSentimentApp.TIMESERIES_TABLE_NAME)
  private TimeseriesTable textSentiments;

  @Path("/aggregates")
  @GET
  public void sentimentAggregates(HttpServiceRequest request, HttpServiceResponder responder) {
    Row row = sentiments.get(new Get("aggregate"));
    Map<byte[], byte[]> result = row.getColumns();
    if (result == null) {
      responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, "No sentiments processed.");
      return;
    }
    Map<String, Long> resp = Maps.newHashMap();
    for (Map.Entry<byte[], byte[]> entry : result.entrySet()) {
      resp.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
    }
    responder.sendJson(resp);
  }

  @Path("/sentiments/{sentiment}")
  @GET
  public void getSentiments(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("sentiment") String sentiment,
                            @DefaultValue("300") @QueryParam("seconds") String seconds,
                            @DefaultValue("10") @QueryParam("limit") String limit) {
    int remaining = Integer.parseInt(limit);

    long time = System.currentTimeMillis();
    long beginTime = time - TimeUnit.MILLISECONDS.convert(Integer.parseInt(seconds), TimeUnit.SECONDS);
    Iterator<TimeseriesTable.Entry> entries = textSentiments.read(sentiment.getBytes(Charsets.UTF_8), beginTime, time);

    Map<String, Long> textTimeMap = Maps.newHashMap();
    while (entries.hasNext()) {
      TimeseriesTable.Entry entry = entries.next();
      if (remaining-- <= 0) {
        break;
      }
      textTimeMap.put(Bytes.toString(entry.getValue()), entry.getTimestamp());
    }
    responder.sendJson(textTimeMap);
  }

  @Path("counts")
  @POST
  public void getCount(HttpServiceRequest request, HttpServiceResponder responder,
                       @DefaultValue("300") @QueryParam("seconds") String seconds) {
    ByteBuffer requestContents = request.getContent();
    String sentimentsData = Charsets.UTF_8.decode(requestContents).toString();
    String[] sentimentArr = GSON.fromJson(sentimentsData, String[].class);


    Map<String, Integer> sentimentCountMap = Maps.newHashMapWithExpectedSize(sentimentArr.length);
    long time = System.currentTimeMillis();
    long beginTime = time - TimeUnit.MILLISECONDS.convert(Integer.parseInt(seconds), TimeUnit.SECONDS);
    for (String sentiment : sentimentArr) {
      Iterator<TimeseriesTable.Entry> entries = textSentiments.read(sentiment.getBytes(Charsets.UTF_8),
                                                                    beginTime, time);
      int count = 0;
      while (entries.hasNext()) {
        entries.next();
        count++;
      }
      sentimentCountMap.put(sentiment, count);
    }
    responder.sendJson(sentimentCountMap);
  }
}
