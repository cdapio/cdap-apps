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

package co.cask.cdap.apps.netlens.app.counter;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.apps.netlens.app.Constants;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 * Counters service handler
 */
public class CountersServiceHandler extends AbstractHttpServiceHandler {
  private static final Gson GSON = new Gson();

  @UseDataSet("counters")
  private TimeseriesTable counters;

  @UseDataSet("trafficCounters")
  private Table trafficCounters;

  @UseDataSet("topN")
  private Table topNTable;

  @GET
  @Path("topN/{startTs}/{limit}")
  public void topN(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("startTs") Long startTs,
                   @PathParam("limit") Long limit) throws IOException {
    doTopN(responder, startTs, limit);
  }

  @GET
  @Path("topN/{startTs}")
  public void topN(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("startTs") Long startTs) throws IOException {
    doTopN(responder, startTs, 10L);
  }

  private void doTopN(HttpServiceResponder responder, Long startTs, Long limit) {
    Table topNTable = this.topNTable;
    byte[] prefix = TrafficCounterFlowlet.TOPN_IP_KEY_PREFIX;
    List<TopNTableUtil.TopNResult> result = TopNTableUtil.get(topNTable, prefix, startTs,
                                                              Constants.TOPN_AGG_INTERVAL_SIZE, limit);
    responder.sendJson(GSON.toJson(result));
  }

  @GET
  @Path("counts/{startTs}/{endTs}/{key}")
  public void timeRange(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("startTs") Long startTs,
                        @PathParam("endTs") Long endTs, @PathParam("key") String key) throws IOException {
    doTimeRange(responder, startTs, endTs, key);
  }

  @GET
  @Path("counts/{startTs}/{endTs}")
  public void timeRange(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("startTs") Long startTs,
                        @PathParam("endTs") Long endTs) throws IOException {
    doTimeRange(responder, startTs, endTs, null);
  }

  private void doTimeRange(HttpServiceResponder responder, Long startTs, Long endTs, String key) throws IOException {
    startTs = (startTs / Constants.AGG_INTERVAL_SIZE) * Constants.AGG_INTERVAL_SIZE;
    if (key != null) {
      getIPsCounts(key, startTs, endTs, responder);
    } else {
      getTrafficCounts(startTs, endTs, responder);
    }
  }

  private void getTrafficCounts(long startTs, long endTs, HttpServiceResponder responder) throws IOException {
    DataPoint[] counts = CounterTableUtil.getCounts(trafficCounters,
                                                    Bytes.EMPTY_BYTE_ARRAY, TrafficCounterFlowlet.TOTAL_COUNTER_COLUMN,
                                                    startTs, endTs);
    responder.sendJson(GSON.toJson(counts));
  }

  private void getIPsCounts(String key, long startTs, long endTs, HttpServiceResponder responder) throws IOException {
    // re-using counters needed for anomaly detection
    Iterator<TimeseriesTable.Entry> entries = counters.read(Bytes.toBytesBinary(key), startTs, endTs);

    int pointsCount = (int) ((endTs - startTs) / Constants.AGG_INTERVAL_SIZE);
    DataPoint[] dataPoints = new DataPoint[pointsCount + 1];
    while (entries.hasNext()) {
      TimeseriesTable.Entry entry = entries.next();
      int index = (int) ((entry.getTimestamp() - startTs) / Constants.AGG_INTERVAL_SIZE);
      dataPoints[index] = new DataPoint(entry.getTimestamp(), Bytes.toInt(entry.getValue()));
    }
    // NOTE: inserting zeroes where value is absent
    for (int i = 0; i < dataPoints.length; i++) {
      if (dataPoints[i] == null) {
        dataPoints[i] = new DataPoint(startTs + i * Constants.AGG_INTERVAL_SIZE, 0);
      }
    }

    responder.sendJson(GSON.toJson(dataPoints));
  }
}
