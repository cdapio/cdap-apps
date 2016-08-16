/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Map reduce job used to compute the bounce counts of visited pages.
 * The mapper uses the stream {@code logEventStream} as input.
 */
public class BounceCountsMapReduce extends AbstractMapReduce {

  @Override
  public void configure() {
    setName("BounceCountsMapReduce");
    setDescription("Bounce Counts MapReduce Program");
  }

  @Override
  public void initialize() throws Exception {
    MapReduceContext context = getContext();    
  
    context.addOutput("bounceCountStore");

    // Retrieve Hadoop Job
    Job job = context.getHadoopJob();

    job.setMapOutputKeyClass(LogInfo.class);

    // We only set IntWritable here to put something, but we are not interested
    // in the map output value class
    job.setMapOutputValueClass(IntWritable.class);

    // Those have to match what the bounceCountsStore Dataset accepts as part of
    // the implementation of the BatchWritable interface
    job.setOutputKeyClass(Void.class);
    job.setOutputValueClass(PageBounce.class);

    job.setMapperClass(BounceCountsMapper.class);
    job.setReducerClass(BounceCountsReducer.class);

    job.setPartitionerClass(NaturalKeyPartitioner.class);
    job.setSortComparatorClass(CompositeKeyComparator.class);

    final long endTime = context.getLogicalStartTime();
    final long startTime = endTime - TimeUnit.MINUTES.toMillis(10);

    // Use the logEventStream as the input of the mapper. We only read the data that has
    // not been read by previous runs
    // This statement forces our Mapper to have as input LongWritable/Text
    StreamBatchReadable.useStreamInput(context, "logEventStream", startTime, endTime);
  }

  /**
   * Bounce Counts Mapper. Parses log events coming from {@code logEventStream} and outputs them.
   */
  public static class BounceCountsMapper extends Mapper<LongWritable, Text, LogInfo, IntWritable> {
    private static final IntWritable OUTPUT_VALUE = new IntWritable(0);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // The body of the stream event is contained in the Text value
      String streamBody = value.toString();
      try {
        LogInfo logInfo = LogInfo.parse(streamBody);
        if (logInfo != null) {
          context.write(logInfo, OUTPUT_VALUE);
        }
      } catch (ParseException e) {
        throw new IOException("Error parsing log event " + streamBody);
      }
    }
  }

  /**
   * Bounce Counts Reducer. Counts the number of times users have bounced from a Web pages.
   */
  public static class BounceCountsReducer extends Reducer<LogInfo, IntWritable, Void, PageBounce> {
    // We consider that if a user has not visited a page in more than 10 minutes,
    // the last page he visited was a bounce
    private static final long PAGE_SESSION_TIMEOUT = TimeUnit.MINUTES.toMillis(10);

    private Map<String, Long> uriCounts = Maps.newHashMap();
    private Map<String, Long> uriBounces = Maps.newHashMap();
    private LogInfo lastLogInfo = null;

    @Override
    protected void reduce(LogInfo key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      String uri = key.getUri();
      increment(uriCounts, uri);

      if (lastLogInfo != null) {

        // Check that the IP address is the same as the last page view
        // If they are the same, check that the last page was reached not too long ago
        if (!lastLogInfo.getIp().equals(key.getIp())
          || lastLogInfo.getTimestamp() + PAGE_SESSION_TIMEOUT < key.getTimestamp()) {
          increment(uriBounces, lastLogInfo.getUri());
        }
      }

      // Update last log seen, for the next call to the reduce method
      lastLogInfo = new LogInfo(key.getIp(), key.getUri(), key.getTimestamp());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      if (lastLogInfo != null) {
        // The last page view has to be a page where the user bounced, otherwise it would not be last
        increment(uriBounces, lastLogInfo.getUri());
      }

      // Output the results to the bounceCountsStore Dataset
      for (Map.Entry<String, Long> uriCount : uriCounts.entrySet()) {
        String uri = uriCount.getKey();
        long visitsCount = uriCount.getValue();
        Long bounceCount = uriBounces.get(uri);
        if (bounceCount == null) {
          bounceCount = 0L;
        }
        context.write(null, new PageBounce(uri, visitsCount, bounceCount));
      }
    }

    private <K> void increment(Map<K, Long> map, K key) {
      Long value = map.get(key);
      if (value == null) {
        value = 1L;
      } else {
        value += 1L;
      }
      map.put(key, value);
    }
  }

  /**
   * Partition {@link LogInfo} using the IP address only so that a reducer gets all URI visited
   * by a particular IP. This is needed to detect which page a user last visited.
   */
  public static class NaturalKeyPartitioner extends Partitioner<LogInfo, IntWritable> {
    @Override
    public int getPartition(LogInfo logInfo, IntWritable value, int numPartitions) {
      int hash = logInfo.getIp().hashCode();
      int partition = Math.abs(hash % numPartitions);
      return partition;
    }
  }

  /**
   * Compares the composite key, {@link LogInfo}.
   * We sort by IP ascendingly and timestamp ascendingly.
   */
  public static class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
      super(LogInfo.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      LogInfo k1 = (LogInfo) w1;
      LogInfo k2 = (LogInfo) w2;

      int result = k1.getIp().compareTo(k2.getIp());
      if(result == 0) {
        result = k1.getTimestamp().compareTo(k2.getTimestamp());
      }
      return result;
    }
  }
}
