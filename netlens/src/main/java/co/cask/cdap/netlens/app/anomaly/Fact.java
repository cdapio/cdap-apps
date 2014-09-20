package co.cask.cdap.netlens.app.anomaly;

import co.cask.cdap.api.common.Bytes;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSortedMap;

import java.util.Map;
import java.util.SortedMap;

/**
 * Represents fact.
 */
public class Fact {
  // map dimension name -> value
  private SortedMap<String, String> dimensions;
  private long ts;

  public Fact(long ts, Map<String, String> dimensions) {
    // todo: avoid copying maps
    this.dimensions = ImmutableSortedMap.copyOf(dimensions);
    this.ts = ts;
  }

  public Map<String, String> getDimensions() {
    return dimensions;
  }

  public long getTs() {
    return ts;
  }

  // this is handy method for re-using same fact instance
  public void setTs(long ts) {
    this.ts = ts;
  }

  public byte[] buildKey() {
    // todo: optimize
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> dimValue : dimensions.entrySet()) {
      sb.append(dimValue.getKey().length()).append(dimValue.getKey());
      sb.append(dimValue.getValue().length()).append(dimValue.getValue());
    }
    return Bytes.toBytes(sb.toString());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(Fact.class)
      .add("ts", ts)
      .add("dimensions", dimensions)
      .toString();
  }
}
