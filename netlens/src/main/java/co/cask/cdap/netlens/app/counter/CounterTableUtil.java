package co.cask.cdap.netlens.app.counter;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.netlens.app.Constants;

/**
 * Provides utility methods for dealing with counters in datasets
 */
// todo: extract custom dataset or use cube;)?
public final class CounterTableUtil {
  public static DataPoint[] getCounts(Table table, byte[] prefix, byte[] column, long startTs, long endTs) {
    int pointsCount = (int) ((endTs - startTs) / Constants.AGG_INTERVAL_SIZE);
    DataPoint[] dataPoints = new DataPoint[pointsCount + 1];

    byte[] startRow = Bytes.add(prefix, Bytes.toBytes(startTs));
    byte[] endRow = Bytes.add(prefix, Bytes.toBytes(endTs));
    Scanner scan = table.scan(startRow, endRow);

    while (true) {
      Row row = scan.next();
      if (row == null) {
        break;
      }
      long ts = Bytes.toLong(row.getRow(), prefix.length);
      int index = (int) ((ts - startTs) / Constants.AGG_INTERVAL_SIZE);
      dataPoints[index] = new DataPoint(ts, row.getLong(column).intValue());
    }

    // NOTE: inserting zeroes where value is absent
    for (int i = 0; i < dataPoints.length; i++) {
      if (dataPoints[i] == null) {
        dataPoints[i] = new DataPoint(startTs + i * Constants.AGG_INTERVAL_SIZE, 0);
      }
    }
    return dataPoints;
  }

}
