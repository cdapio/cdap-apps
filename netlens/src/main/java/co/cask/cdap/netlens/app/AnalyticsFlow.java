package co.cask.cdap.netlens.app;

import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.netlens.app.anomaly.AnomalyDetectionFlowlet;
import co.cask.cdap.netlens.app.anomaly.AnomalyFanOutFlowlet;
import co.cask.cdap.netlens.app.anomaly.FactParser;
import co.cask.cdap.netlens.app.counter.AnomalyCounterFlowlet;
import co.cask.cdap.netlens.app.counter.TrafficCounterFlowlet;
import co.cask.cdap.netlens.app.histo.NumberCategorizationFlowlet;

/**
 *
 */
public class AnalyticsFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("AnalyticsFlow")
      .setDescription("Performs analysis of network packets.")
      .withFlowlets()
        .add("fact-parser", new FactParser())
        .add("categorize-numbers", new NumberCategorizationFlowlet())
        .add("anomaly-fanout", new AnomalyFanOutFlowlet())
        .add("anomaly-detect", new AnomalyDetectionFlowlet())
        .add("anomaly-count", new AnomalyCounterFlowlet())
        .add("traffic-count", new TrafficCounterFlowlet())
      .connect()
        .from(new Stream("packets")).to("fact-parser")
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
