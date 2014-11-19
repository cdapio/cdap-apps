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

package co.cask.cdap.apps.netlens.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.TimeseriesTables;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.apps.netlens.app.counter.AnomaliesCountService;
import co.cask.cdap.apps.netlens.app.anomaly.AnomaliesService;
import co.cask.cdap.apps.netlens.app.counter.CountersService;

import java.util.concurrent.TimeUnit;

/**
 * Network traffic analytics application.
 */
public class NetlensApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("Netlens");
    setDescription("Network traffic analytics application.");

    // Network data is pushed to a stream
    addStream(new Stream("packets"));

    // Network data analysis flow (processing).
    addFlow(new AnalyticsFlow());

    // "anomalies" dataset keeps detected anomalies
    TimeseriesTables.createTable(getConfigurer(), "anomalies", (int) TimeUnit.MINUTES.toMillis(5));

    // "anomalyCounters" dataset keeps time-aggregated counters of detected anomalies
    TimeseriesTables.createTable(getConfigurer(), "anomalyCounters", (int) TimeUnit.MINUTES.toMillis(5));

    // "trafficCounters" dataset keeps traffic stats
    createDataset("trafficCounters", Table.class);

    // "uniqueCounters" dataset keeps time-aggregated counters for unique ips seen in network traffic
    createDataset("uniqueCounters", Table.class);

    // "topN" dataset keeps topN indexes for most frequent ips and ips that have most anomalies detected
    createDataset("topN", Table.class);

    // "counters" dataset keeps aggregated counters for different combinations of
    TimeseriesTables.createTable(getConfigurer(), "counters", (int) TimeUnit.MINUTES.toMillis(5));

    // Service to serve anomalies stats
    addService(new AnomaliesCountService());
    // Service to serve anomalies details
    addService(new AnomaliesService());
    // Service to serve traffic stats requests
    addService(new CountersService());

  }
}
