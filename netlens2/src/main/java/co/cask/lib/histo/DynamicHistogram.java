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

package co.cask.lib.histo;

import org.apache.commons.math3.stat.inference.ChiSquareTest;

import java.util.Arrays;

public class DynamicHistogram {
  private final int numInitialDataPoints;
  private final int numInitialDataPointsPerBucket;
  private final int numBuckets;
  private final int halfLife;
  private long numDataPoints;
  private double initialDataPoints[];
  private final double meanLifetime;

  private double min;
  private double max;
  private Bucket buckets[];

  /**
   * @param numBuckets                    number of buckets. If there are 10 buckets then the output histogram
   *                                      will have 10 percentile ranges.
   * @param numInitialDataPointsPerBucket If there are 10 buckets and numInitialDataPointsPerBucket
   *                                      is 100 then the histogram is seeded with the first 1000 points
   * @param halfLife                      how fast does the count of a bucket decays. If this is set to 1000 then a bucket's count will be
   *                                      halved if it is not updated in 1000 consecutive updates
   */
  public DynamicHistogram(int numBuckets, int numInitialDataPointsPerBucket, int halfLife) {
    this.halfLife = halfLife;
    this.numBuckets = numBuckets;
    if (numBuckets < 3) {
      numBuckets = 3;
    }
    buckets = new Bucket[numBuckets];
    if (numInitialDataPointsPerBucket < 0) {
      numInitialDataPointsPerBucket = 1;
    }
    this.numInitialDataPointsPerBucket = numInitialDataPointsPerBucket;
    this.numInitialDataPoints = numInitialDataPointsPerBucket * numBuckets;
    this.numDataPoints = 0;
    this.initialDataPoints = new double[numInitialDataPoints];
    // http://en.wikipedia.org/wiki/Exponential_decay
    this.meanLifetime = 1.44 * halfLife;
  }

  private void createInitialHistogram() {
    Arrays.sort(initialDataPoints);
    buckets = new Bucket[numBuckets];
    for (int i = 0; i < numBuckets - 1; i++) {
      int offset;
      buckets[i] = new Bucket();
      offset = (i + 1) * numInitialDataPointsPerBucket;
      buckets[i].high = (initialDataPoints[offset - 1] + initialDataPoints[offset]) / 2;
      buckets[i].count = numInitialDataPointsPerBucket;
    }
    buckets[numBuckets - 1] = new Bucket();
    buckets[numBuckets - 1].high = Double.POSITIVE_INFINITY;
    buckets[numBuckets - 1].count = numInitialDataPointsPerBucket;
    min = initialDataPoints[0];
    max = initialDataPoints[numInitialDataPoints - 1];
    return;
  }

  private void addInitialDataPoint(double d) {
    assert (numDataPoints < numInitialDataPoints);
    initialDataPoints[(int) numDataPoints] = d;
    numDataPoints++;
    if (numDataPoints == numInitialDataPoints) {
      createInitialHistogram();
      initialDataPoints = null;
    }
    return;
  }

  public int findBucketIndex(double d) {
    if (this.numDataPoints < this.numInitialDataPoints) {
      return -1;
    }
    for (int i = 0; ; i++) {
      if (d < buckets[i].high) {
        return i;
      }
    }
  }

  public synchronized void addDataPoint(double d) {
    if (numDataPoints < numInitialDataPoints) {
      addInitialDataPoint(d);
      return;
    }
    int index = findBucketIndex(d);
    long noUpdateDuration = numDataPoints - buckets[index].lastUpdateAt;
    numDataPoints++;
    buckets[index].lastUpdateAt = numDataPoints;
    double oldCount = buckets[findBucketIndex(d)].count;
    /* Keep track of mean and standard deviation in the edge buckets FIXME */
    buckets[index].count = 1.0 + oldCount * Math.exp(-noUpdateDuration / meanLifetime);
    if (min > d) {
      min = d;
    }
    if (max < d) {
      max = d;
    }

    repartitionIfNeeded();

    return;
  }

  private void repartitionIfNeeded() {
    // repartitioning every <halfLife> points
    if (numDataPoints % halfLife == 0) {
      if (isTooSkewed()) {
        repartition();
      }
    }
  }

  public synchronized boolean isTooSkewed() {
    if (this.numDataPoints < this.numInitialDataPoints) {
      return false;
    }
    double totalCount = 0.0;
    for (int i = 0; i < numBuckets; i++) {
      totalCount += buckets[i].count;
    }
    double averageCount = totalCount / numBuckets;
    double expected[] = new double[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
      expected[i] = averageCount;
    }
    long observed[] = new long[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
      observed[i] = Math.round(buckets[i].count);
    }
    ChiSquareTest test = new ChiSquareTest();
    // true if reject hypothesis "observed same as expected" with 95% confidence
    return (test.chiSquareTest(expected, observed, .05));
  }

  public synchronized void repartition() {
    double totalCount = 0.0;
    for (int i = 0; i < numBuckets; i++) {
      totalCount += buckets[i].count;
    }
    double averageCount = totalCount / numBuckets;
    double density[] = new double[numBuckets];
    // FIXME use data distribution to estimate density, especially for edge buckets
    density[0] = buckets[0].count / (buckets[0].high - min);
    for (int i = 1; i < numBuckets - 1; i++) {
      density[i] = buckets[i].count / (buckets[i].high - buckets[i - 1].high);
    }
    density[numBuckets - 1] = buckets[numBuckets - 1].count / (max - buckets[numBuckets - 2].high);
    int curBucket = 0;
    double marker = min;
    long lastUpdateAt = 0;
    double availInCurBucket = buckets[curBucket].count;
    Bucket newBuckets[] = new Bucket[numBuckets];
    for (int i = 0; i < numBuckets - 1; i++) {
      double remaining = averageCount;
      do {
        if (lastUpdateAt < buckets[curBucket].lastUpdateAt) {
          lastUpdateAt = buckets[curBucket].lastUpdateAt;
        }
        if (availInCurBucket > remaining) {
          marker = marker + remaining / density[curBucket];
          assert (marker < buckets[curBucket].high);
          availInCurBucket -= remaining;
          remaining = 0;
        } else if (availInCurBucket < remaining) {
          assert (buckets[curBucket].high > marker);
          marker = buckets[curBucket].high;
          remaining -= availInCurBucket;
          curBucket++;
          availInCurBucket = buckets[curBucket].count;
        } else {
          assert (buckets[curBucket].high > marker);
          marker = buckets[curBucket].high;
          remaining = 0;
          curBucket++;
          availInCurBucket = buckets[curBucket].count;
        }
      } while (remaining > 0);
      newBuckets[i] = new Bucket();
      newBuckets[i].high = marker;
      newBuckets[i].count = averageCount;
      newBuckets[i].lastUpdateAt = lastUpdateAt;
      lastUpdateAt = buckets[curBucket].lastUpdateAt;
    }
    newBuckets[numBuckets - 1] = new Bucket();
    newBuckets[numBuckets - 1].high = Double.POSITIVE_INFINITY;
    newBuckets[numBuckets - 1].count = averageCount;
    newBuckets[numBuckets - 1].lastUpdateAt = Math.max(lastUpdateAt, buckets[numBuckets - 1].lastUpdateAt);
    buckets = newBuckets;
    return;
  }

  public Bucket[] getHistogram() {
    if (this.numDataPoints < this.numInitialDataPoints) {
      return (null);
    }
    Bucket[] b = new Bucket[numBuckets + 1];
    b[0] = new Bucket();
    b[0].count = 0;
    b[0].high = min;
    for (int i = 1; i < numBuckets; i++) {
      b[i] = buckets[i - 1];
    }
    b[numBuckets] = new Bucket();
    b[numBuckets].count = buckets[numBuckets - 1].count;
    b[numBuckets].high = max;
    return b;
  }

  public static final class Bucket {
    public double high;
    public double count;
    private long lastUpdateAt;

    public double getHigh() {
      return high;
    }

    public double getCount() {
      return count;
    }

    public long getLastUpdateAt() {
      return lastUpdateAt;
    }
  }
}
