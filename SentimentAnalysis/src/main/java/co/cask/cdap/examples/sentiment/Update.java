package co.cask.cdap.examples.sentiment;


import co.cask.cdap.api.ResourceSpecification;
import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletSpecification;
import co.cask.cdap.api.metrics.Metrics;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Updates the timeseries table with sentiments received.
 */
public class Update extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(Update.class);
  private static final Gson GSON = new Gson();

  @UseDataSet("sentiments")
  private Table sentiments;

  @UseDataSet("text-sentiments")
  private TimeseriesTable textSentiments;

  Metrics metrics;

  @Batch(10)
  @ProcessInput("sentiments")
  public void process(Iterator<Tweet> sentimentItr) {
    while (sentimentItr.hasNext()) {
      Tweet tweet = sentimentItr.next();
      String sentence = tweet.getText();
      String sentiment = tweet.getSentiment();
      metrics.count("sentiment." + sentiment, 1);
      sentiments.increment(new Increment("aggregate", sentiment, 1));
      textSentiments.write(new TimeseriesTable.Entry(sentiment.getBytes(Charsets.UTF_8),
                                                     sentence.getBytes(Charsets.UTF_8),
                                                     tweet.getCreatedAt()));

    }
  }

  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName("Update")
      .setDescription("Updates the sentiment counts")
      .withResources(ResourceSpecification.BASIC)
      .build();
  }
}