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

package co.cask.cdap.apps.sentiment;

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Normalizes the sentences.
 */
public class Normalization extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(Normalization.class);

  /**
   * Emitters for emitting sentences from this Flowlet.
   */
  @Output("python")
  private OutputEmitter<Tweet> pythonOut;

  /**
   * Handler to emit metrics.
   */
  Metrics metrics;

  public void initialize(FlowletContext context) throws Exception {
    // no-op
  }


  @Batch(100)
  @ProcessInput
  public void processEvents(StreamEvent event) {
    String text = Bytes.toString(Bytes.toBytes(event.getBody()));
    if (text != null) {
      metrics.count("data.processed.size", text.length());
      Tweet tweet = new Tweet(text, System.currentTimeMillis());
      emit(tweet);
    } else {
      metrics.count("data.ignored.text", 1);
    }
  }

  @Batch(100)
  @ProcessInput
  public void processTweets(Tweet tweet) {
    emit(tweet);
  }

  private void emit(Tweet tweet) {
    pythonOut.emit(tweet);
  }
}
