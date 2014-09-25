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

package co.cask.cdap.apps.netlens.app.anomaly;

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
