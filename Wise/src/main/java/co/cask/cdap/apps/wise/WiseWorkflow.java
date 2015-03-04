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
package co.cask.cdap.apps.wise;

import co.cask.cdap.api.workflow.AbstractWorkflow;

/**
 * Implements a simple Workflow with one Workflow action to run the BounceCountsMapReduce
 * with a schedule that runs every 10 minutes.
 */
public class WiseWorkflow extends AbstractWorkflow {

  @Override
  public void configure() {
      setName("WiseWorkflow");
      setDescription("Wise Workflow");
      addMapReduce("BounceCountsMapReduce");
  }
}
