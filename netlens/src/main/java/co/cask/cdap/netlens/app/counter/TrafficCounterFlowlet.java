package co.cask.cdap.netlens.app.counter;

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.netlens.app.Constants;
import co.cask.cdap.netlens.app.anomaly.Fact;

import java.util.Iterator;

/**
 *
 */
public class TrafficCounterFlowlet extends AbstractFlowlet {
  public static final byte[] TOTAL_COUNTER_COLUMN = Bytes.toBytes("T");
  public static final byte[] TOPN_IP_KEY_PREFIX = Bytes.toBytes("tip_");


  @UseDataSet("trafficCounters")
  private Table trafficCounters;

  @UseDataSet("topN")
  private Table topNTable;

  @Batch(100)
  @ProcessInput
  public void process(Iterator<Fact> facts) {
    while (facts.hasNext()) {
      process(facts.next());
    }
  }

  private void process(Fact fact) {
    count(fact);
    countTopN(fact);
  }

  private void count(Fact fact) {
    long ts = (fact.getTs() / Constants.AGG_INTERVAL_SIZE) * Constants.AGG_INTERVAL_SIZE;
    trafficCounters.increment(Bytes.toBytes(ts), TOTAL_COUNTER_COLUMN, 1);
  }

  private void countTopN(Fact fact) {
    String src = fact.getDimensions().get("src");
    if (src != null) {
      TopNTableUtil.add(topNTable, TOPN_IP_KEY_PREFIX,
                        Bytes.toBytes(src), fact.getTs(),
                        Constants.TOPN_AGG_INTERVAL_SIZE * 10, Constants.TOPN_AGG_INTERVAL_SIZE);
    }
  }
}
