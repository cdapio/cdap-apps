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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 * Anomalies service handler
 */
public class AnomaliesServiceHandler extends AbstractHttpServiceHandler {

  private static final Gson GSON = new Gson();

  @UseDataSet("anomalies")
  private TimeseriesTable anomalies;

  @GET
  @Path("timeRange/{startTs}/{endTs}/{groupFor}/{src}")
  public void timeRange(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("startTs") Long startTs,
                        @PathParam("endTs") Long endTs, @PathParam("groupFor") String groupFor,
                        @PathParam("src") String src) throws IOException {
    if("none".equals(groupFor))
      groupFor = null;
    doTimeRange(responder, startTs, endTs, groupFor, src);
  }

  @GET
  @Path("timeRange/{startTs}/{endTs}/{groupFor}")
  public void timeRange(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("startTs") Long startTs,
                        @PathParam("endTs") Long endTs, @PathParam("groupFor") String groupFor) throws IOException {
    doTimeRange(responder, startTs, endTs, groupFor, null);
  }

  @GET
  @Path("timeRange/{startTs}/{endTs}")
  public void timeRange(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("startTs") Long startTs,
                        @PathParam("endTs") Long endTs) throws IOException {
    doTimeRange(responder, startTs, endTs, null, null);
  }

  private void doTimeRange(HttpServiceResponder responder, Long startTs, Long endTs, String groupFor, String src) {
    Map<String, String> filterBy = src == null ? null : ImmutableMap.of("src", src);
    List<Anomaly> anomalies = getAnomalies(endTs, startTs, filterBy, groupFor);
    // we want most recent on top
    Collections.reverse(anomalies);
    responder.sendJson(GSON.toJson(anomalies));
  }

  private List<Anomaly> getAnomalies(long endTs, long startTs, String groupFor) {
    Iterator<TimeseriesTable.Entry> entries = this.anomalies.read(AnomalyDetectionFlowlet.ANOMALY_KEY, startTs, endTs);
    if (groupFor == null) {
      return getAnomalies(entries);
    } else {
      return getAnomalies(entries, groupFor);
    }
  }

  // filter is dim:value "AND" "exact match"
  private List<Anomaly> getAnomalies(long endTs, long startTs, @Nullable Map<String, String> filter, String groupFor) {
    List<Anomaly> anomalies = getAnomalies(endTs, startTs, groupFor);

    if (filter == null) {
      return anomalies;
    }

    List<Anomaly> filtered = Lists.newArrayList();
    for (Anomaly anomaly : anomalies) {
      for (Map.Entry<String, String> filterByField : filter.entrySet()) {
        if (filterByField.getValue().equals(anomaly.fact.getDimensions().get(filterByField.getKey()))) {
          filtered.add(anomaly);
        }
      }
    }

    return filtered;
  }

  private List<Anomaly> getAnomalies(Iterator<TimeseriesTable.Entry> entries) {
    List<Anomaly> facts = Lists.newArrayList();
    while (entries.hasNext()) {
      TimeseriesTable.Entry entry = entries.next();
      Fact fact = GSON.fromJson(Bytes.toString(entry.getTags()[0]), Fact.class);
      String key = Bytes.toStringBinary(entry.getValue());
      facts.add(new Anomaly(key, fact));
    }
    return facts;
  }

  private List<Anomaly> getAnomalies(Iterator<TimeseriesTable.Entry> entries, String groupFor) {
    // map of ts -> anomaly group -> anomaly details
    // NOTE: has to bee sorted map to keep order by time
    Map<Long, Map<String, Anomaly>> facts = Maps.newTreeMap();
    while (entries.hasNext()) {
      TimeseriesTable.Entry entry = entries.next();
      Fact fact = GSON.fromJson(Bytes.toString(entry.getTags()[0]), Fact.class);
      String key = Bytes.toStringBinary(entry.getValue());

      String groupingValue = fact.getDimensions().get(groupFor);
      if (groupingValue == null) {
        continue;
      }

      // groups for timestamp
      Map<String, Anomaly> groups = facts.get(fact.getTs());
      if (groups == null) {
        groups = Maps.newHashMap();
        facts.put(fact.getTs(), groups);
      }

      Anomaly group = groups.get(groupingValue);
      if (group == null) {
        groups.put(groupingValue, new Anomaly(key, fact));
        continue;
      }

      merge(group.fact, fact, groupFor);
    }

    // we need to return time ordered list of anomalies
    List<Anomaly> result = Lists.newArrayList();
    for (Map.Entry<Long, Map<String, Anomaly>> groups : facts.entrySet()) {
      for (Anomaly anomaly : groups.getValue().values()) {
        result.add(anomaly);
      }
    }

    return result;
  }

  private void merge(Fact existing, Fact toApply, String skipGrouping) {
    for (Map.Entry<String, String> field : toApply.getDimensions().entrySet()) {
      if (skipGrouping.equals(field.getKey())) {
        continue;
      }
      // grouping (merging) logic is the simplest: we fill fact fields with value if there's single value for it in the
      // whole group, otherwise we put "[grouped]" as a value
      String existingValue = existing.getDimensions().get(field.getKey());
      if (existingValue != null && !existingValue.equals(field.getValue())) {
        existing.getDimensions().put(field.getKey(), "[grouped]");
      } else {
        existing.getDimensions().put(field.getKey(), field.getValue());
      }
    }
  }

  // defines the format of response
  private static class Anomaly {
    private String dataSeriesKey;
    private Fact fact;

    private Anomaly(String dataSeriesKey, Fact fact) {
      this.dataSeriesKey = dataSeriesKey;
      this.fact = fact;
    }
  }
}
