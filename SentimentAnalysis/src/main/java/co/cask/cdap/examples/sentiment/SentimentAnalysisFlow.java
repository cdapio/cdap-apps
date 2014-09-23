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

}
