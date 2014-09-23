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
package co.cask.cdap.examples.sentiment;

import co.cask.cdap.api.ResourceSpecification;
import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.FlowletSpecification;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.apps.flowlet.ExternalProgramFlowlet;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Flow for sentiment analysis.
 */
public class SentimentAnalysisFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("SentimentAnalysis")
      .setDescription("Analysis of text to generate sentiments")
      .withFlowlets()
        .add(new TweetCollector())
        .add(new Normalization())
        .add(new Analyze())
        .add(new Analysis())
        .add(new Update())
      .connect()
        .fromStream("sentence").to(new Normalization())
        .from(new Normalization()).to(new Analyze())
        .from(new TweetCollector()).to(new Analyze())
        .from(new Analyze()).to(new Update())
        .from(new TweetCollector()).to(new Analysis())
        .from(new Analysis()).to(new Update())
      .build();
  }

  /**
   * Normalizes the sentences.
   */
  public static class Normalization extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(Normalization.class);

    /**
     * Emitter for emitting sentences from this Flowlet.
     */
    private OutputEmitter<Tweet> out;

    /**
     * Handler to emit metrics.
     */
    Metrics metrics;

    @Batch(100)
    @ProcessInput
    public void process(StreamEvent event) {
      String text = Bytes.toString(Bytes.toBytes(event.getBody()));
      if (text != null) {
        metrics.count("data.processed.size", text.length());
        Tweet tweet = new Tweet(text, System.currentTimeMillis());
        out.emit(tweet);
      } else {
        metrics.count("data.ignored.text", 1);
      }
    }
  }

  /**
   * Analyzes the sentences by passing the sentence to NLTK based sentiment analyzer
   * written in Python.
   */
  public static class Analyze extends ExternalProgramFlowlet<Tweet, Tweet> {
    private static final Logger LOG = LoggerFactory.getLogger(Normalization.class);
    private static final Gson GSON = new Gson();

    @Output("sentiments")
    private OutputEmitter<Tweet> sentiment;

    private File workDir;

    /**
     * This method will be called at Flowlet initialization time.
     *
     * @param context The {@link co.cask.cdap.api.flow.flowlet.FlowletContext} for this Flowlet.
     * @return An {@link co.cask.cdap.flow.flowlet.ExternalProgramFlowlet.ExternalProgram} to specify
     * properties of the external program to process input.
     */
    @Override
    protected ExternalProgram init(FlowletContext context) {
      try {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("sentiment-process.zip");

        if (in != null) {
          workDir = new File("work");
          Unzipper.unzip(in, workDir);

          File python = new File("/usr/bin/python");
          File program = new File(workDir, "sentiment/score_sentiment.py");

          if (python.exists()) {
            return new ExternalProgram(python, program.getAbsolutePath());
          }
        }

        throw new RuntimeException("Unable to start process");
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    /**
     * This method will be called for each input event to transform the given input into string before sending to
     * external program for processing.
     *
     * @param input The input event.
     * @return A UTF-8 encoded string of the input, or {@code null} if to skip this input.
     */
    @Override
    protected String encode(Tweet input) {
      return GSON.toJson(input);
    }

    /**
     * This method will be called when the external program returns the result. Child class can do its own processing
     * in this method or could return an object of type {@code OUT} for emitting to next Flowlet with the
     * {@link co.cask.cdap.api.flow.flowlet.OutputEmitter} returned by {@link #getOutputEmitter()}.
     *
     * @param result The result from the external program.
     * @return The output to emit or {@code null} if nothing to emit.
     */
    @Override
    protected Tweet processResult(String result) {
      return GSON.fromJson(result, Tweet.class);
    }

    /**
     * Child class can override this method to return an OutputEmitter for writing data to the next Flowlet.
     *
     * @return An {@link co.cask.cdap.api.flow.flowlet.OutputEmitter} for type {@code OUT}, or {@code null} if
     * this flowlet doesn't have output.
     */
    @Override
    protected OutputEmitter<Tweet> getOutputEmitter() {
      return sentiment;
    }

    @Override
    protected void finish() {
      if (workDir == null) {
        return;
      }
      try {
        LOG.info("Deleting work dir {}", workDir);
        FileUtils.deleteDirectory(workDir);
      } catch (IOException e) {
        LOG.error("Could not delete work dir {}", workDir);
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Updates the timeseries table with sentiments received.
   */
  public static class Update extends AbstractFlowlet {
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
}
