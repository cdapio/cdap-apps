/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;


/**
 * Anomalies count service handler
 */
public class AnomaliesCountServiceHandler extends AbstractHttpServiceHandler {

  @UseDataSet("anomalyCounters")
  private TimeseriesTable anomalyCounters;

  @UseDataSet("uniqueCounters")
  private Table uniqueCounters;

  @UseDataSet("topN")
  private Table topNTable;

  @GET
  @Path("count/{startTs}/{endTs}")
  public void count(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("startTs") Long startTs,
                    @PathParam("endTs") Long endTs, @QueryParam("src") String src) throws IOException {
    List<DataPoint> dataPoints;
    if (src != null) {
      dataPoints = getCounts(anomalyCounters, startTs, endTs,
                             Bytes.add(AnomalyCounterFlowlet.IP_COUNTER_KEY_PREFIX, Bytes.toBytes(src)));
    } else {
      dataPoints = getCounts(anomalyCounters, startTs, endTs, AnomalyCounterFlowlet.TOTAL_COUNTER_KEY_PREFIX);
    }
    responder.sendJson(dataPoints);
  }

  @GET
  @Path("uniqueIpsCount/{startTs}/{endTs}")
  public void uniquesCount(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("startTs") Long startTs, @PathParam("endTs") Long endTs) throws IOException {
    List<DataPoint> dataPoints = CounterTableUtil.getCounts(uniqueCounters,
                                                            AnomalyCounterFlowlet.UNIQUE_IP_ANOMALY_COUNT_KEY_PREFIX,
                                                            AnomalyCounterFlowlet.UNIQUE_IP_ANOMALY_COUNT_COLUMN,
                                                            startTs, endTs);
    responder.sendJson(dataPoints);
  }

  @GET
  @Path("topN/{startTs}")
  public void topN(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("startTs") Long startTs,
                   @DefaultValue("10") @QueryParam("limit") Long limit) throws IOException {
    Table topNTable = this.topNTable;
    byte[] prefix = AnomalyCounterFlowlet.TOPN_IP_WITH_ANOMALIES_KEY_PREFIX;
    List<TopNTableUtil.TopNResult> result = TopNTableUtil.get(topNTable, prefix, startTs,
                                                              Constants.TOPN_AGG_INTERVAL_SIZE, limit);
    responder.sendJson(result);
  }

  private static List<DataPoint> getCounts(TimeseriesTable tsTable, long startTs, long endTs, byte[] seriesKey) {
    // NOTE: inserting zeroes where value is absent
    int pointsCount = (int) ((endTs - startTs) / Constants.AGG_INTERVAL_SIZE);
    // normalizing startTs
    startTs = (startTs / Constants.AGG_INTERVAL_SIZE) * Constants.AGG_INTERVAL_SIZE;
    // ts -> # of entries
    Map<Long, Integer> data = Maps.newTreeMap();
    // we want zeroes to be filled
    for (int i = 0; i < pointsCount; i++) {
      long ts = startTs + i * Constants.AGG_INTERVAL_SIZE;
      data.put(ts, 0);
    }

    // counting
    Iterator<TimeseriesTable.Entry> entries = tsTable.read(seriesKey, startTs, endTs);
    while (entries.hasNext()) {
      TimeseriesTable.Entry entry = entries.next();
      long ts = entry.getTimestamp();
      Integer count = data.get(ts);
      count = count == null ? 0 : count;
      data.put(ts, count + 1);
    }

    // preparing output
    List<DataPoint> dataPoints = Lists.newArrayList();
    for (Map.Entry<Long, Integer> item : data.entrySet()) {
      dataPoints.add(new DataPoint(item.getKey(), item.getValue()));
    }
    return dataPoints;
  }
}
