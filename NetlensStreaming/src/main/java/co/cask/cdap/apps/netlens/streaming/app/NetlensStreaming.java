  package co.cask.cdap.apps.netlens.streaming.app;

import co.cask.cdap.apps.netlens.streaming.app.histo.DynamicHistogram;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 * <p>
 * Usage: NetlensStreaming <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example org.apache.spark.examples.streaming.NetlensStreaming localhost 9999`
 */
public final class NetlensStreaming {
  private static final Logger LOG = LoggerFactory.getLogger(NetlensStreaming.class);
  private static final Gson GSON = new Gson();
  private static final String[] CATEGORIES = {"low", "medium", "high"};
  public static final String ACCEPTED_DIMENSIONS = "acceptDims";

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: NetlensStreaming <hostname> <port>");
      System.exit(1);
    }

    // Create a local StreamingContext with two working thread and batch interval of 1 second
    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetleansStreaming");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

    // Create a DStream that will connect to hostname:port, like localhost:9999
    JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[0], Integer.parseInt(args[1]));

    /**
     * Parses stream data, outputs {@link Fact}s.
     *
     * src: source ip
     * spt: source port
     * dst: destination ip
     * dpt: destination port
     * app: traffic type, e.g. TCP
     * rt: request time - long
     * rs: request size (missing from our logs) - int
     * rl: latency (missing from our logs) - int
     * ipv: internet protocol e.g. IPv4
     *
     * atz: source timezone
     * dtz: destination timezone
     */
    JavaDStream<Fact> facts = lines.map(new FactConvertor());

    // Categorizes number values (ints, doubles, etc.)
    JavaDStream<Fact> catFacts = facts.map(new NumberCategorization());

    JavaDStream<Fact> anomalyFanOut = catFacts.flatMap(new AnomalyFanOut());

    anomalyFanOut.print();

    jssc.start();
    jssc.awaitTermination();
  }

  private static class FactConvertor implements Function<String, Fact> {
    @Override
    public Fact call(String line) throws Exception {
      Map<String, String> map = GSON.fromJson(line, new TypeToken<Map<String, String>>(){}.getType());
      return new Fact(System.currentTimeMillis(), map);
    }
  }

  private static class NumberCategorization implements Function<Fact, Fact> {
    @Override
    public Fact call(Fact fact) throws Exception {
      Map<String, DynamicHistogram> histograms = new HashMap<String, DynamicHistogram>();
      histograms.put("rl", new DynamicHistogram(CATEGORIES.length, 300, 100));
      histograms.put("rs", new DynamicHistogram(CATEGORIES.length, 300, 100));
      for (Map.Entry<String, DynamicHistogram> histo : histograms.entrySet()) {
        String value = fact.getDimensions().get(histo.getKey());
        if (value != null) {
          double d = Double.valueOf(value);
          histo.getValue().addDataPoint(d);
          int bucketIndex = histo.getValue().findBucketIndex(d);
          if (bucketIndex >= 0) {
            fact.getDimensions().put(histo.getKey(), CATEGORIES[bucketIndex]);
          } else {
            fact.getDimensions().remove(histo.getKey());
          }
        }
      }
      return fact;
    }
  }

  private static class AnomalyFanOut implements FlatMapFunction<Fact, Fact> {
    @Override
    public Iterable<Fact> call(Fact fact) throws Exception {
      List<Fact> facts = new LinkedList<Fact>();
      Set<String> requiredDimensions = Sets.newHashSet("src");
      Set<String> acceptedDimensions = Sets.newHashSet(requiredDimensions);

      Set<String> propertyDims = new Gson().fromJson(new Gson().toJson(ImmutableSet.of("src", "rt", "app", "dst", "atz", "ahost")),
                                                     new TypeToken<Set<String>>() {}.getType());

      acceptedDimensions.addAll(propertyDims);

//        propertyDims = new Gson().fromJson(context.getRuntimeArguments().get(ACCEPTED_DIMENSIONS),
//                                           new TypeToken<Set<String>>() {}.getType());
      if (propertyDims != null) {
        acceptedDimensions.addAll(propertyDims);
      }

      LOG.info("Required Dimensions {}", requiredDimensions);
      LOG.info("Accepted Dimensions {}", acceptedDimensions);

      Map<String, String> required = Maps.newTreeMap();
      Map<String, String> notRequired = Maps.newTreeMap();
      for (Map.Entry<String, String> dimVal: fact.getDimensions().entrySet()) {
        if (requiredDimensions.contains(dimVal.getKey())) {
          required.put(dimVal.getKey(), dimVal.getValue());
        } else {
          notRequired.put(dimVal.getKey(), dimVal.getValue());
        }
      }

      // todo: make dim set size limit configurable
      List<Map<String, String>> subsetsOfNonRequired = getAllSubsets(notRequired, 2);

      for (Map<String, String> subsetOfNonRequired : subsetsOfNonRequired) {
        subsetOfNonRequired.putAll(required);
        Fact toEmit = new Fact(fact.getTs(), subsetOfNonRequired);
        facts.add(toEmit);
      }
      return facts;
    }

    static List<Map<String, String>> getAllSubsets(Map<String, String> original, int maxSubsetSize) {
      // start with empty
      List<Map<String, String>> subsets = Lists.newArrayList();

      for (Map.Entry<String, String> item : original.entrySet()) {
        // adding all same as existing subsets but with new item
        List<Map<String, String>> newSubsets = Lists.newArrayList();
        for (Map<String, String> subset : subsets) {
          if (subset.size() == maxSubsetSize) {
            continue;
          }
          Map<String, String> copy = Maps.newTreeMap();
          copy.putAll(subset);
          copy.put(item.getKey(), item.getValue());
          newSubsets.add(copy);
        }
        subsets.addAll(newSubsets);

        // adding another one with only new item
        Map<String, String> withItem = Maps.newTreeMap();
        withItem.put(item.getKey(), item.getValue());
        subsets.add(withItem);
      }

      // adding empty one - also a subset, right? ;)
      subsets.add(Maps.<String, String>newTreeMap());

      return subsets;
    }
  }
}
