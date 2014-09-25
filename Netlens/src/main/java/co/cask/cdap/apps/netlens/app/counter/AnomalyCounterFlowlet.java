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

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.FlowletException;
import co.cask.cdap.apps.netlens.app.Constants;
import co.cask.cdap.apps.netlens.app.anomaly.Fact;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class AnomalyCounterFlowlet extends AbstractFlowlet {
  public static final byte[] TOTAL_COUNTER_KEY_PREFIX = Bytes.toBytes("T_");
  public static final byte[] IP_COUNTER_KEY_PREFIX = Bytes.toBytes("IC_");

  public static final byte[] IP_ANOMALY_LAST_SEEN_KEY_PREFIX = Bytes.toBytes("IS_");
  public static final byte[] IP_ANOMALY_LAST_SEEN_COLUMN = Bytes.toBytes("ls");
  public static final byte[] UNIQUE_IP_ANOMALY_COUNT_KEY_PREFIX = Bytes.toBytes("UI_");
  public static final byte[] UNIQUE_IP_ANOMALY_COUNT_COLUMN = Bytes.toBytes("un");

  // todo: move such constants of same dataset into one place?
  public static final byte[] TOPN_IP_WITH_ANOMALIES_KEY_PREFIX = Bytes.toBytes("atip_");

  public static final byte[] FOO_VALUE = new byte[1];

  @UseDataSet("anomalyCounters")
  private TimeseriesTable anomalyCounters;

  @UseDataSet("uniqueCounters")
  private Table uniqueCounters;

  @UseDataSet("topN")
  private Table topNTable;

  @Override
  public void initialize(FlowletContext context) throws FlowletException {
    lastSeenCache = Maps.newHashMap();
  }

  @Batch(100)
  @ProcessInput
  public void process(Iterator<Fact> anomalies) {
    while (anomalies.hasNext()) {
      process(anomalies.next());
    }
  }

  private void process(Fact anomaly) {
    countAnomalies(anomaly);
    countUniqueIps(anomaly);
    countTopIps(anomaly);
  }

  private void countAnomalies(Fact anomaly) {
    // placing key into a tag: simplest de-duping
    byte[] tag = anomaly.buildKey();
    // for totals
    anomalyCounters.write(new TimeseriesTable.Entry(TOTAL_COUNTER_KEY_PREFIX, FOO_VALUE, anomaly.getTs(), tag));
    // per ip
    String ip = anomaly.getDimensions().get("src");
    anomalyCounters.write(new TimeseriesTable.Entry(Bytes.add(IP_COUNTER_KEY_PREFIX, Bytes.toBytes(ip)),
                                                    FOO_VALUE, anomaly.getTs(), tag));
  }

  private void countTopIps(Fact fact) {
    String src = fact.getDimensions().get("src");
    if (src != null) {
      TopNTableUtil.add(topNTable, TOPN_IP_WITH_ANOMALIES_KEY_PREFIX,
                        Bytes.toBytes(src), fact.getTs(),
                        Constants.TOPN_AGG_INTERVAL_SIZE * 10, Constants.TOPN_AGG_INTERVAL_SIZE);
    }
  }

  // todo: move below methods into UniqCounterTable
  private void countUniqueIps(Fact anomaly) {
    String ip = anomaly.getDimensions().get("src");
    if(!seenAt(ip, anomaly.getTs())) {
      uniqueCounters.increment(Bytes.add(UNIQUE_IP_ANOMALY_COUNT_KEY_PREFIX, Bytes.toBytes(anomaly.getTs())),
                               UNIQUE_IP_ANOMALY_COUNT_COLUMN, 1);
    }
  }

  // todo: use txnl MemoryTable
  private Map<String, Long> lastSeenCache;

  private boolean seenAt(String ip, long ts) {
    Long lastTs = lastSeenCache.get(ip);
    if (lastTs == null) {
      lastTs = uniqueCounters
        .get(new Get(Bytes.add(IP_ANOMALY_LAST_SEEN_KEY_PREFIX, Bytes.toBytes(ip)), IP_ANOMALY_LAST_SEEN_COLUMN))
        .getLong(IP_ANOMALY_LAST_SEEN_COLUMN);
    }

    if (lastTs == null || lastTs < ts) {
      uniqueCounters
        .put(new Put(Bytes.add(IP_ANOMALY_LAST_SEEN_KEY_PREFIX, Bytes.toBytes(ip)), IP_ANOMALY_LAST_SEEN_COLUMN, ts));
      lastSeenCache.put(ip, ts);
      return false;
    }

    return true;
  }

}
