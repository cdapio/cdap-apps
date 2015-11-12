/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.flow.AbstractFlow;

/**
 * Flow for sentiment analysis.
 */
public class SentimentAnalysisFlow extends AbstractFlow {
  static final String FLOW_NAME = "TwitterSentimentAnalysis";

  @Override
  protected void configure() {
    setName(FLOW_NAME);
    setDescription("Analysis of text to generate sentiments");
    addFlowlet(new TweetCollector());
    addFlowlet(new TweetParserFlowlet());
    addFlowlet(new PythonAnalyzer());
    addFlowlet(new CountSentimentFlowlet());
    connectStream(TwitterSentimentApp.STREAM_NAME, new TweetParserFlowlet());
    connect(new TweetParserFlowlet(), new PythonAnalyzer());
    connect(new TweetCollector(), new PythonAnalyzer());
    connect(new PythonAnalyzer(), new CountSentimentFlowlet());
  }
}
