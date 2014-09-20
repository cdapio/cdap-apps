package co.cask.cdap.netlens.app.anomaly;

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.FlowletException;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.netlens.app.Constants;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class AnomalyDetectionFlowlet extends AbstractFlowlet {
  // todo: make configurable, move out of this class
  public static final byte[] ANOMALY_KEY = Bytes.toBytes("a");

  private static final Gson GSON = new Gson();

  private OutputEmitter<Fact> output;

  @UseDataSet("counters")
  private TimeseriesTable counters;

  @UseDataSet("anomalies")
  private TimeseriesTable anomalies;

  // todo: use MemoryTable to make cache transactional
  private long cachedTs;
  private Set<String> cachedAnomalies;

  @Override
  public void initialize(FlowletContext context) throws FlowletException {
    cachedAnomalies = Sets.newHashSet();
  }

  @Batch(100)
  @ProcessInput
  public void process(Iterator<Fact> facts) {
    while (facts.hasNext()) {
      process(facts.next());
    }
  }

  private void process(Fact fact) {
    int lastToCompareWith = 10;

    // First, fetch last <lastToCompareWith> data points.
    // Second, increment current data point
    // Third, figure out if current data point is anomaly

    // round up
    long ts = (fact.getTs() / Constants.AGG_INTERVAL_SIZE) * Constants.AGG_INTERVAL_SIZE;

    // 1)
    // fetching <lastToCompareWith + 1> including current data point so that we can increment without round-trip to
    // table
    byte[] key = fact.buildKey();

    long previousStartTs = ts - Constants.AGG_INTERVAL_SIZE * (lastToCompareWith);
    // todo: internally simpleTimeSeriesTable uses column range scan which is not efficient
    List<TimeseriesTable.Entry> lastEntries = counters.read(key,
                                                            previousStartTs,
                                                            ts);

    int[] counts = getCounts(lastEntries, previousStartTs, Constants.AGG_INTERVAL_SIZE, lastToCompareWith + 1);

    // 2)
    // last one is current data point
    int currentCount = counts[counts.length - 1] + 1;
    TimeseriesTable.Entry incremented = new TimeseriesTable.Entry(key, Bytes.toBytes(currentCount), ts);
    counters.write(incremented);

    // 3)
    if (isLastPointAnomaly(counts, 2.0, 2.0)) {
      fact.setTs(ts);
      if (!isDuplicate(fact, ts)) {
        anomalies.write(new TimeseriesTable.Entry(ANOMALY_KEY, key, fact.getTs(), Bytes.toBytes(GSON.toJson(fact))));
        output.emit(fact);
      }
    }
  }

  private int[] getCounts(List<TimeseriesTable.Entry> entries, long startTs, long intervalSize, int count) {
    int[] counters = new int[count];
    for (TimeseriesTable.Entry entry : entries) {
      int index = (int) ((entry.getTimestamp() - startTs) / intervalSize);
      counters[index] = Bytes.toInt(entry.getValue());
    }

    return counters;
  }

  static boolean isLastPointAnomaly(int[] counts, double meanThreshold, double sensitivity) {
    // Simple algo: Xn+1 is anomaly if (mean(X1..Xn) - Xn+1) > deviation(X1..Xn) * sensitivity
    double mean = mean(counts, 0, counts.length);
    if (mean > meanThreshold) {
      double stdDev = standardDeviation(counts, 0, counts.length - 1);
      int last = counts[counts.length - 1];
      return (last - mean) > sensitivity * stdDev;
    }

    return false;
  }

  private static double mean(int[] counts, int offset, int count) {
    double sum = 0;
    for (int i = offset; i < offset + count; i++) {
      sum += counts[i];
    }

    return count == 0 ? 0 : sum / count;
  }

  private static double standardDeviation(int[] counts, int offset, int count) {
    double stdDev = 0;
    double mean  = mean(counts, offset, count);
    for (int i = offset; i < offset + count; i++) {
      stdDev += Math.pow(counts[i] - mean, 2);
    }
    return count == 0 ? 0 : Math.sqrt(stdDev / count);
  }

  private boolean isDuplicate(Fact fact, long ts) {
    String key = Bytes.toStringBinary(fact.buildKey());
    if (cachedTs != ts) {
      cachedAnomalies.clear();
      cachedAnomalies.add(key);
      cachedTs = ts;
      return false;
    }

    if (cachedAnomalies.contains(key)) {
      return true;
    } else {
      cachedAnomalies.add(key);
      return false;
    }
  }
}
