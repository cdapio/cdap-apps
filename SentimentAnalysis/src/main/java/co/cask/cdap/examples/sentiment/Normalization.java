package co.cask.cdap.examples.sentiment;

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Normalizes the sentences.
 */
public class Normalization extends AbstractFlowlet {
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
