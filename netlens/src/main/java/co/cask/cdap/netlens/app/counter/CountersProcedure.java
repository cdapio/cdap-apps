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
import com.google.gson.Gson;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class CountersProcedure extends AbstractProcedure {
  private static final Gson GSON = new Gson();

  @UseDataSet("counters")
  private TimeseriesTable counters;

  @UseDataSet("trafficCounters")
  private Table trafficCounters;

  @UseDataSet("topN")
  private Table topNTable;

  @Handle("topN")
  public void topN(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    // the interval length is fixed for top N, so we only need startTs
    Preconditions.checkArgument(request.getArgument("startTs") != null, "Missing required argument: 'startTs'");

    long startTs = Long.valueOf(request.getArgument("startTs"));
    String limitArg = request.getArgument("limit");
    long limit = limitArg == null ? 100 : Long.valueOf(limitArg);
    Table topNTable = this.topNTable;
    byte[] prefix = TrafficCounterFlowlet.TOPN_IP_KEY_PREFIX;

    List<TopNTableUtil.TopNResult> result = TopNTableUtil.get(topNTable, prefix, startTs,
                                                              Constants.TOPN_AGG_INTERVAL_SIZE, limit);

    responder.sendJson(GSON.toJson(result));
  }

  @Handle("counts")
  public void timeRange(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    Preconditions.checkArgument(request.getArgument("startTs") != null, "Missing required argument: 'startTs'");
    Preconditions.checkArgument(request.getArgument("endTs") != null, "Missing required argument: 'endTs'");

    long startTs = Long.valueOf(request.getArgument("startTs"));
    startTs = (startTs / Constants.AGG_INTERVAL_SIZE) * Constants.AGG_INTERVAL_SIZE;
    long endTs = Long.valueOf(request.getArgument("endTs"));

    String key = request.getArgument("key");

    if (key != null) {
      getIPsCounts(key, startTs, endTs, responder);
    } else {
      getTrafficCounts(startTs, endTs, responder);
    }
  }

  private void getTrafficCounts(long startTs, long endTs, ProcedureResponder responder) throws IOException {
    DataPoint[] counts = CounterTableUtil.getCounts(trafficCounters,
                                                    Bytes.EMPTY_BYTE_ARRAY, TrafficCounterFlowlet.TOTAL_COUNTER_COLUMN,
                                                    startTs, endTs);

    responder.sendJson(GSON.toJson(counts));
  }

  private void getIPsCounts(String key, long startTs, long endTs, ProcedureResponder responder) throws IOException {
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
