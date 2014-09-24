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

package co.cask.cdap.netlens.app.histo;

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.FlowletException;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.lib.histo.DynamicHistogram;
import co.cask.cdap.netlens.app.anomaly.Fact;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;

/**
 * Categorizes number values (ints, doubles, etc.)
 *
 * NOTE: this needs to warm-up before it can do categorization. Until then, it will remove values meant to be
 *       categorized
 */
public class NumberCategorizationFlowlet extends AbstractFlowlet {
  private static final String[] CATEGORIES = {"low", "medium", "high"};

  private OutputEmitter<Fact> output;

  private Map<String, DynamicHistogram> histograms;

  @Override
  public void initialize(FlowletContext context) throws FlowletException {
    // todo: make configurable
    histograms = Maps.newHashMap();
    histograms.put("rl", new DynamicHistogram(CATEGORIES.length, 300, 100));
    histograms.put("rs", new DynamicHistogram(CATEGORIES.length, 300, 100));
  }

  @Batch(100)
  @ProcessInput
  public void categorize(Iterator<Fact> facts) {
    while (facts.hasNext()) {
      categorize(facts.next());
    }
  }

  private void categorize(Fact fact) {
    for (Map.Entry<String, DynamicHistogram> histo : histograms.entrySet()) {
      String value = fact.getDimensions().get(histo.getKey());
      if (value != null) {
        double d = Double.valueOf(value);
        histo.getValue().addDataPoint(d);
        int bucketIndex = histo.getValue().findBucketIndex(d);
        if (bucketIndex >= 0) {
          fact.getDimensions().put(histo.getKey(), CATEGORIES[bucketIndex]);
        } else {
          fact.getDimensions().remove(histo.getKey());
        }
      }
    }
    output.emit(fact);
  }
}
