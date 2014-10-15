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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Table;

/**
 * Application that analyzes sentiment of sentences as positive, negative or neutral.
 */
public class TwitterSentimentApp extends AbstractApplication {
  static final String APP_NAME = "TwitterSentiment";
  static final String STREAM_NAME = "TweetStream";
  static final String TABLE_NAME = "TotalCounts";
  static final String TIMESERIES_TABLE_NAME = "TimeSeriesTable";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("Twitter Sentiment Analysis");
    addStream(new Stream(STREAM_NAME));
    createDataset(TABLE_NAME, Table.class);
    createDataset(TIMESERIES_TABLE_NAME, TimeseriesTable.class);
    addFlow(new SentimentAnalysisFlow());
    addProcedure(new SentimentQueryProcedure());
  }
}
