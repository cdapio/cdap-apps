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

package co.cask.cdap.apps.netlens.app;

import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.apps.netlens.app.anomaly.AnomalyDetectionFlowlet;
import co.cask.cdap.apps.netlens.app.anomaly.AnomalyFanOutFlowlet;
import co.cask.cdap.apps.netlens.app.anomaly.FactParser;
import co.cask.cdap.apps.netlens.app.counter.AnomalyCounterFlowlet;
import co.cask.cdap.apps.netlens.app.counter.TrafficCounterFlowlet;
import co.cask.cdap.apps.netlens.app.histo.NumberCategorizationFlowlet;

/**
 *
 */
public class AnalyticsFlow extends AbstractFlow {
  static final String NAME = "AnalyticsFlow";

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("Performs analysis of network packets.");

    addFlowlet("fact-parser", new FactParser());
    addFlowlet("categorize-numbers", new NumberCategorizationFlowlet());
    addFlowlet("anomaly-fanout", new AnomalyFanOutFlowlet());
    addFlowlet("anomaly-detect", new AnomalyDetectionFlowlet());
    addFlowlet("anomaly-count", new AnomalyCounterFlowlet());
    addFlowlet("traffic-count", new TrafficCounterFlowlet());

    connectStream(NetlensApp.STREAM_NAME, "fact-parser");
    // anomaly subtree
    connect("fact-parser", "categorize-numbers");
    connect("categorize-numbers", "anomaly-fanout");
    connect("anomaly-fanout", "anomaly-detect");
    // anomaly counters
    connect("anomaly-detect", "anomaly-count");
    // counters subtree
    connect("fact-parser", "traffic-count");
  }
}
