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

package co.cask.cdap.netlens.app.counter;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Provides utility methods for counting top N  stuff in datasets
 */
// todo: extract custom dataset or use cube;)?
public final class TopNTableUtil {
  public static final byte[] TOPN_KEY_PREFIX = Bytes.toBytes("t");
  public static final byte[] TOPN_FOO_COLUMN = Bytes.toBytes("f");
  public static final byte[] TOPN_INDEX_KEY_PREFIX = Bytes.toBytes("i");
  public static final byte[] TOPN_INDEX_COLUMN = Bytes.toBytes("k");

  public static final byte[] FOO_VALUE = new byte[1];

  public static void add(Table topNTable, byte[] rowPrefix, byte[] value, long ts,
                         long aggIntervalSize, long granularity) {
    // todo: use parameter granularity instead of constant
    ts = (ts / granularity) * granularity;
    // NOTE: for topN rows we use key of format:
    //       <TOPN_KEY_PREFIX><interval_start_ts as long><inverted_count as int><value>

    byte[] topNRowPrefix = Bytes.add(rowPrefix, TOPN_KEY_PREFIX);
    byte[] indexRowPrefix = Bytes.add(rowPrefix, TOPN_INDEX_KEY_PREFIX);
    // count for every interval of <aggIntervalSize> size that is started every <granularity> and covers <ts> point
    for (long intervalStart = ts; intervalStart > ts - aggIntervalSize; intervalStart -= granularity) {
      byte[] intervalStartBytes = Bytes.toBytes(intervalStart);
      byte[] indexKey = Bytes.add(indexRowPrefix, intervalStartBytes, value);
      // todo: use cache (table caches only inside one tx)
      // todo: fetch keys for all intervals at once outside the loop
      byte[] topNKey = topNTable.get(indexKey, TOPN_INDEX_COLUMN);
      int count;
      if (topNKey == null) {
        count = 0;
      } else {
        count = Integer.MAX_VALUE - Bytes.toInt(topNKey, topNRowPrefix.length + Bytes.SIZEOF_LONG);
        topNTable.delete(topNKey, TOPN_FOO_COLUMN);
      }

      count++;

      // write new row into topN
      int invertedCount = Integer.MAX_VALUE - count;
      topNKey = Bytes.add(topNRowPrefix, intervalStartBytes);
      topNKey = Bytes.add(topNKey, Bytes.toBytes(invertedCount), value);
      topNTable.put(topNKey, TOPN_FOO_COLUMN, FOO_VALUE);

      // updating index
      topNTable.put(indexKey, TOPN_INDEX_COLUMN, topNKey);
    }
  }

  // todo: granularity is needed only to round up startTs...
  public static List<TopNResult> get(Table topNTable, byte[] rowPrefix, long startTs, long granularity, long limit) {
    // todo: use parameter instead of constant
    startTs = (startTs / granularity) * granularity;

    List<TopNResult> result = Lists.newArrayList();
    // NOTE: for topN rows we use key of format:
    //       <prefix><interval_start_ts as long><inverted_count as int><value>

    byte[] topNRowPrefix = Bytes.add(rowPrefix, TOPN_KEY_PREFIX);
    byte[] startKey = Bytes.add(topNRowPrefix, Bytes.toBytes(startTs));
    byte[] stopKey = Bytes.add(topNRowPrefix, Bytes.toBytes(startTs + 1));
    Scanner scan = topNTable.scan(startKey, stopKey);
    while (limit > 0) {
      Row row = scan.next();
      if (row == null) {
        break;
      }
      int count = Integer.MAX_VALUE - Bytes.toInt(row.getRow(),
                                                  topNRowPrefix.length + Bytes.SIZEOF_LONG);
      int metaSize = topNRowPrefix.length + Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT;
      byte[] val = Bytes.tail(row.getRow(), row.getRow().length - metaSize);
      result.add(new TopNResult(Bytes.toString(val), count));
      limit--;
    }
    return result;
  }

  // for serializing response
  public static final class TopNResult {
    private String value;
    private int count;

    public TopNResult(String value, int count) {
      this.value = value;
      this.count = count;
    }
  }

}
