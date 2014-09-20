package co.cask.cdap.netlens.app.counter;

/**
 * Represents timeseries data point.
 */
public class DataPoint {
  private long ts;
  private int value;

  public DataPoint(long ts, int value) {
    this.ts = ts;
    this.value = value;
  }

  public long getTs() {
    return ts;
  }

  public void setTs(long ts) {
    this.ts = ts;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }
}
