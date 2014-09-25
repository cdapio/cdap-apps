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

package co.cask.cdap.apps.netlens.app.anomaly;

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.FlowletException;
import co.cask.cdap.api.flow.flowlet.FlowletSpecification;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class AnomalyFanOutFlowlet extends AbstractFlowlet {
  public static final String ACCEPTED_DIMENSIONS = "acceptDims";
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyFanOutFlowlet.class);

  @Output("out")
  private OutputEmitter<Fact> output;

  private Set<String> requiredDimensions;
  // TODO: these are not used
  private Set<String> acceptedDimensions;

  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName("fanout")
      .setDescription("Fanout flowlet")
      .withArguments(ImmutableMap.of(ACCEPTED_DIMENSIONS,
                                     new Gson().toJson(ImmutableSet.of("src", "rt", "app", "dst", "atz", "ahost"))))
      .build();
  }

  @Override
  public void initialize(FlowletContext context) throws FlowletException {
    requiredDimensions = Sets.newHashSet();
    requiredDimensions.add("src");

    // Pickup accepted dimensions from the spec properties and runtime config
    acceptedDimensions = Sets.newHashSet();
    acceptedDimensions.addAll(requiredDimensions);

    Set<String> propertyDims = new Gson().fromJson(context.getSpecification().getProperty(ACCEPTED_DIMENSIONS),
                                                   new TypeToken<Set<String>>() {}.getType());

    acceptedDimensions.addAll(propertyDims);

    propertyDims = new Gson().fromJson(context.getRuntimeArguments().get(ACCEPTED_DIMENSIONS),
                                       new TypeToken<Set<String>>() {}.getType());
    if (propertyDims != null) {
      acceptedDimensions.addAll(propertyDims);
    }

    LOG.info("Required Dimensions {}", requiredDimensions);
    LOG.info("Accepted Dimensions {}", acceptedDimensions);
  }

  @Batch(100)
  @ProcessInput
  public void process(Iterator<Fact> facts) {
    while (facts.hasNext()) {
      process(facts.next());
    }
  }

  private void process(Fact fact) {
    Map<String, String> required = Maps.newTreeMap();
    Map<String, String> notRequired = Maps.newTreeMap();
    for (Map.Entry<String, String> dimVal: fact.getDimensions().entrySet()) {
      if (requiredDimensions.contains(dimVal.getKey())) {
        required.put(dimVal.getKey(), dimVal.getValue());
      } else {
        notRequired.put(dimVal.getKey(), dimVal.getValue());
      }
    }

    // todo: make dim set size limit configurable
    List<Map<String, String>> subsetsOfNonRequired = getAllSubsets(notRequired, 2);

    for (Map<String, String> subsetOfNonRequired : subsetsOfNonRequired) {
      subsetOfNonRequired.putAll(required);
      Fact toEmit = new Fact(fact.getTs(), subsetOfNonRequired);
      output.emit(toEmit);
    }
  }

  static List<Map<String, String>> getAllSubsets(Map<String, String> original, int maxSubsetSize) {
    // start with empty
    List<Map<String, String>> subsets = Lists.newArrayList();

    for (Map.Entry<String, String> item : original.entrySet()) {
      // adding all same as existing subsets but with new item
      List<Map<String, String>> newSubsets = Lists.newArrayList();
      for (Map<String, String> subset : subsets) {
        if (subset.size() == maxSubsetSize) {
          continue;
        }
        Map<String, String> copy = Maps.newTreeMap();
        copy.putAll(subset);
        copy.put(item.getKey(), item.getValue());
        newSubsets.add(copy);
      }
      subsets.addAll(newSubsets);

      // adding another one with only new item
      Map<String, String> withItem = Maps.newTreeMap();
      withItem.put(item.getKey(), item.getValue());
      subsets.add(withItem);
    }

    // adding empty one - also a subset, right? ;)
    subsets.add(Maps.<String, String>newTreeMap());

    return subsets;
  }
}
