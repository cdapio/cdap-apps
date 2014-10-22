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
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;

/**
 * Normalizes the sentences.
 */
public class TweetParserFlowlet extends AbstractFlowlet {

  /**
   * Emitters for emitting sentences from this Flowlet.
   */
  private OutputEmitter<Tweet> out;

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
      out.emit(tweet);
    } else {
      metrics.count("data.ignored.text", 1);
    }
  }

}
