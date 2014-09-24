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

package co.cask.cdap.netlens.app.counter;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.netlens.app.Constants;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AnomalyCountsProcedure extends AbstractProcedure {
  private static final Gson GSON = new Gson();

  @UseDataSet("anomalyCounters")
  private TimeseriesTable anomalyCounters;

  @UseDataSet("uniqueCounters")
  private Table uniqueCounters;

  @UseDataSet("topN")
  private Table topNTable;

  @Handle("count")
  public void count(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    Preconditions.checkArgument(request.getArgument("startTs") != null, "Missing required argument: 'startTs'");
    Preconditions.checkArgument(request.getArgument("endTs") != null, "Missing required argument: 'endTs'");

    long startTs = Long.valueOf(request.getArgument("startTs"));
    long endTs = Long.valueOf(request.getArgument("endTs"));
    String src = request.getArgument("src");

    DataPoint[] dataPoints;
    if (src != null) {
      dataPoints = getCounts(anomalyCounters, startTs, endTs,
                             Bytes.add(AnomalyCounterFlowlet.IP_COUNTER_KEY_PREFIX, Bytes.toBytes(src)));
    } else {
      dataPoints = getCounts(anomalyCounters, startTs, endTs, AnomalyCounterFlowlet.TOTAL_COUNTER_KEY_PREFIX);
    }

    responder.sendJson(GSON.toJson(dataPoints));
  }

  @Handle("uniqueIpsCount")
  public void uniquesCount(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    Preconditions.checkArgument(request.getArgument("startTs") != null, "Missing required argument: 'startTs'");
    Preconditions.checkArgument(request.getArgument("endTs") != null, "Missing required argument: 'endTs'");

    long startTs = Long.valueOf(request.getArgument("startTs"));
    long endTs = Long.valueOf(request.getArgument("endTs"));

    DataPoint[] dataPoints = CounterTableUtil.getCounts(uniqueCounters,
                                                        AnomalyCounterFlowlet.UNIQUE_IP_ANOMALY_COUNT_KEY_PREFIX,
                                                        AnomalyCounterFlowlet.UNIQUE_IP_ANOMALY_COUNT_COLUMN,
                                                        startTs, endTs);
    responder.sendJson(GSON.toJson(dataPoints));
  }

  @Handle("topN")
  public void topN(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    // the interval length is fixed for top N, so we only need startTs
    Preconditions.checkArgument(request.getArgument("startTs") != null, "Missing required argument: 'startTs'");

    long startTs = Long.valueOf(request.getArgument("startTs"));
    String limitArg = request.getArgument("limit");
    long limit = limitArg == null ? 100 : Long.valueOf(limitArg);
    Table topNTable = this.topNTable;
    byte[] prefix = AnomalyCounterFlowlet.TOPN_IP_WITH_ANOMALIES_KEY_PREFIX;

    List<TopNTableUtil.TopNResult> result = TopNTableUtil.get(topNTable, prefix, startTs,
                                                              Constants.TOPN_AGG_INTERVAL_SIZE, limit);

    responder.sendJson(GSON.toJson(result));
  }

  private static DataPoint[] getCounts(TimeseriesTable tsTable, long startTs, long endTs, byte[] seriesKey) {
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
    DataPoint[] dataPoints = new DataPoint[data.size()];
    Iterator<Map.Entry<Long,Integer>> it = data.entrySet().iterator();
    int index = 0;
    while (it.hasNext()) {
      Map.Entry<Long, Integer> item = it.next();
      dataPoints[index++] = new DataPoint(item.getKey(), item.getValue());
    }
    return dataPoints;
  }

}
