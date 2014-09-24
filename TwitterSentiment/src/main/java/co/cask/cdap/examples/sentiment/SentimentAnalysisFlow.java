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

import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;

/**
 * Flow for sentiment analysis.
 */
public class SentimentAnalysisFlow implements Flow {
  static final String FLOW_NAME = "TwitterSentimentAnalysis";
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName(FLOW_NAME)
      .setDescription("Analysis of text to generate sentiments")
      .withFlowlets()
        .add(new TweetCollector())
        .add(new Normalization())
        .add(new PythonAnalyzer())
        .add(new JavaAnalyzer())
        .add(new Update())
      .connect()
        .fromStream(TwitterSentimentApp.STREAM_NAME).to(new Normalization())
        .from(new Normalization()).to(new JavaAnalyzer())
        .from(new Normalization()).to(new PythonAnalyzer())
        .from(new TweetCollector()).to(new Normalization())
        .from(new PythonAnalyzer()).to(new Update())
        .from(new JavaAnalyzer()).to(new Update())
      .build();
  }

}
