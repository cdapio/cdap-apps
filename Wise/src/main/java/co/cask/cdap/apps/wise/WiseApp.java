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
package co.cask.cdap.apps.wise;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

/**
 * The Wise application performs analytics on Apache access logs.
 */
public class WiseApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("Wise");
    setDescription("Web Insights Engine");
    // The logEventStream stream will receive the access logs
    addStream(new Stream("logEventStream"));
    // Custom Dataset to store basic page view information by IP address
    createDataset("pageViewStore", PageViewStore.class);
    // Custom Dataset to store bounce information for every web page
    createDataset("bounceCountStore", BounceCountStore.class);
    // Utility Dataset used in the Map/Reduce job
    createDataset("bounceCountsMapReduceLastRun", KeyValueTable.class);
    // Add the WiseFlow flow
    addFlow(new WiseFlow());
    // Add the WiseWorkflow workflow
    addWorkflow(new WiseWorkflow());
    // Add the WiseService service
    addService(new WiseService());
  }
}
