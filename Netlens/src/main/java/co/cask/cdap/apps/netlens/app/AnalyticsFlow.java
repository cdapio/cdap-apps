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

import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.apps.netlens.app.anomaly.AnomalyDetectionFlowlet;
import co.cask.cdap.apps.netlens.app.anomaly.AnomalyFanOutFlowlet;
import co.cask.cdap.apps.netlens.app.anomaly.FactParser;
import co.cask.cdap.apps.netlens.app.counter.AnomalyCounterFlowlet;
import co.cask.cdap.apps.netlens.app.counter.TrafficCounterFlowlet;
import co.cask.cdap.apps.netlens.app.histo.NumberCategorizationFlowlet;

/**
 *
 */
public class AnalyticsFlow implements Flow {
  static final String FLOW_NAME = "AnalyticsFlow";

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName(FLOW_NAME)
      .setDescription("Performs analysis of network packets.")
      .withFlowlets()
        .add("fact-parser", new FactParser())
        .add("categorize-numbers", new NumberCategorizationFlowlet())
        .add("anomaly-fanout", new AnomalyFanOutFlowlet())
        .add("anomaly-detect", new AnomalyDetectionFlowlet())
        .add("anomaly-count", new AnomalyCounterFlowlet())
        .add("traffic-count", new TrafficCounterFlowlet())
      .connect()
        .fromStream(NetlensApp.STREAM_NAME).to("fact-parser")
        // anomaly subtree
        .from("fact-parser").to("categorize-numbers")
        .from("categorize-numbers").to("anomaly-fanout")
        .from("anomaly-fanout").to("anomaly-detect")
        // anomaly counters
        .from("anomaly-detect").to("anomaly-count")
        // counters subtree
        .from("fact-parser").to("traffic-count")
      .build();
  }
}
