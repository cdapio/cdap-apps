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
package co.cask.cdap.apps.wise;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Scannables;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Custom Dataset storing the bounce counts of different Web pages.
 */
public class BounceCountStore extends AbstractDataset
  implements BatchWritable<Void, PageBounce>,
             RecordScannable<PageBounce> {
  static final byte[] COL_VISITS = new byte[] { 'v' };
  static final byte[] COL_BOUNCES = new byte[] { 'b' };

  // Define the underlying table
  private final Table table;

  public BounceCountStore(DatasetSpecification spec, @EmbeddedDataset("bounces") Table table) {
    super(spec.getName(), table);
    this.table = table;
  }

  /**
   * Increment a bounce count entry with the specified number of visits and bounces.
   *
   * @param uri URI of the Web page
   * @param visits number of visits to add to the Web page
   * @param bounces number of bounces to add to the Web page
   */
  public void increment(String uri, long visits, long bounces) {
    table.increment(Bytes.toBytes(uri),
                    new byte[][] { COL_VISITS, COL_BOUNCES },
                    new long[] { visits, bounces });
  }

  /**
   * Retrieve a bounce count entry from this {@link BounceCountStore}.
   *
   * @param uri URI of the Web page
   * @return the bounce count entry associated to the Web page with the {@code uri}
   */
  public PageBounce get(String uri) {
    Row row = table.get(Bytes.toBytes(uri), new byte[][] { COL_VISITS, COL_BOUNCES });
    if (row.isEmpty()) {
      return new PageBounce(uri, 0, 0);
    }
    long visits = Bytes.toLong(row.get(COL_VISITS));
    long bounces = Bytes.toLong(row.get(COL_BOUNCES));
    return new PageBounce(uri, visits, bounces);
  }

  @Override
  public Type getRecordType() {
    return PageBounce.class;
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public RecordScanner<PageBounce> createSplitRecordScanner(Split split) {
    return Scannables.splitRecordScanner(table.createSplitReader(split), new PageBounceRecordMaker());
  }

  @Override
  public void write(Void ignored, PageBounce pageBounce) {
    this.increment(pageBounce.getUri(), pageBounce.getTotalVisits(), pageBounce.getBounces());
  }

  /**
   * {@link co.cask.cdap.api.data.batch.Scannables.RecordMaker} for {@link #createSplitRecordScanner(Split)}.
   */
  public class PageBounceRecordMaker implements Scannables.RecordMaker<byte[], Row, PageBounce> {
    @Override
    public PageBounce makeRecord(byte[] key, Row row) {
      long visits = Bytes.toLong(row.get(COL_VISITS));
      long bounces = Bytes.toLong(row.get(COL_BOUNCES));
      return new PageBounce(Bytes.toString(key), visits, bounces);
    }
  }
}
