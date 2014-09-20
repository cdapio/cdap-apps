package co.cask.cdap.netlens.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.TimeseriesTables;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.netlens.app.anomaly.AnomaliesProcedure;
import co.cask.cdap.netlens.app.counter.AnomalyCountsProcedure;
import co.cask.cdap.netlens.app.counter.CountersProcedure;

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

    // Procedure to serve traffic stats requests
    addProcedure(new CountersProcedure());
    // Procedure to serve anomalies details
    addProcedure(new AnomaliesProcedure());
    // Procedure to serve anomalies stats
    addProcedure(new AnomalyCountsProcedure());
  }
}
