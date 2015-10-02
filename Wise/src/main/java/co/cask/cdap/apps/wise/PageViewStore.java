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

package co.cask.cdap.apps.wise;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A custom-defined Dataset is used to track page views by IP addresses.
 */
public class PageViewStore extends AbstractDataset
  implements RecordScannable<KeyValue<String, Map<String, Long>>> {

  // Define the underlying table
  private Table table;

  public PageViewStore(DatasetSpecification spec, @EmbeddedDataset("tracks") Table table) {
    super(spec.getName(), table);
    this.table = table;
  }

  /**
   * Increment the count of a page view by 1.
   *
   * @param logInfo log information
   */
  public void incrementCount(LogInfo logInfo) {
    table.increment(new Increment(logInfo.getIp(), logInfo.getUri(), 1L));
  }

  /**
   * Get the count of visited pages viewed from a specified IP address.
   *
   * @param ipAddress IP address used to look for visited page counts
   * @return visited page URIs and their counts
   */
  public Map<String, Long> getPageCount(String ipAddress) {
    Row row = this.table.get(new Get(ipAddress));
    Map<String, Long> pageCount = getPageCounts(row);
    return pageCount;
  }

  /**
   * Get the total number of visited pages viewed from a specified IP address.
   *
   * @param ipAddress IP address used to look for visited pages counts
   * @return the number of visited pages
   */
  public long getCounts(String ipAddress) {
    Row row = this.table.get(new Get(ipAddress));
    if (row.isEmpty()) {
      return 0;
    }
    int count = 0;
    for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
      count += Bytes.toLong(entry.getValue());
    }
    return count;
  }

  @Override
  public Type getRecordType() {
    return new TypeToken<KeyValue<String, Map<String, Long>>>() { }.getType();
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public RecordScanner<KeyValue<String, Map<String, Long>>> createSplitRecordScanner(Split split) {
    return new RecordScanner<KeyValue<String, Map<String, Long>>>() {
      private SplitReader<byte[], Row> splitReader;

      @Override
      public void initialize(Split split) throws InterruptedException {
        this.splitReader = table.createSplitReader(split);
        this.splitReader.initialize(split);
      }

      @Override
      public boolean nextRecord() throws InterruptedException {
        return this.splitReader.nextKeyValue();
      }

      @Override
      public KeyValue<String, Map<String, Long>> getCurrentRecord() throws InterruptedException {
        String ip = Bytes.toString(this.splitReader.getCurrentKey());
        Row row = this.splitReader.getCurrentValue();
        Map<String, Long> pageCount = getPageCounts(row);
        return new KeyValue<String, Map<String, Long>>(ip, pageCount);
      }

      @Override
      public void close() {
        this.splitReader.close();
      }
    };
  }

  private Map<String, Long> getPageCounts(Row row) {
    if (row == null || row.isEmpty()) {
      return null;
    }
    Map<String, Long> pageCount = new HashMap<String, Long>();
    for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
      pageCount.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
    }
    return pageCount;
  }
}
