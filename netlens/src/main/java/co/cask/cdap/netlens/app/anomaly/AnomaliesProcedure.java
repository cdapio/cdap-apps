package co.cask.cdap.netlens.app.anomaly;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AnomaliesProcedure extends AbstractProcedure {
  private static final Gson GSON = new Gson();

  @UseDataSet("anomalies")
  private TimeseriesTable anomalies;

  @Handle("timeRange")
  public void timeRange(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    Preconditions.checkArgument(request.getArgument("startTs") != null, "Missing required argument: 'startTs'");
    Preconditions.checkArgument(request.getArgument("endTs") != null, "Missing required argument: 'endTs'");

    long startTs = Long.valueOf(request.getArgument("startTs"));
    long endTs = Long.valueOf(request.getArgument("endTs"));
    String src = request.getArgument("src");
    // NOTE: this is not analogue to "GROUP BY" but rather smth like "group for"
    String groupFor = request.getArgument("groupFor");

    Map<String, String> filterBy = src == null ? null : ImmutableMap.of("src", src);

    List<Anomaly> anomalies = getAnomalies(endTs, startTs, filterBy, groupFor);

    // we want most recent on top
    Collections.reverse(anomalies);
    responder.sendJson(GSON.toJson(anomalies));
  }

  private List<Anomaly> getAnomalies(long endTs, long startTs, String groupFor) {
    List<TimeseriesTable.Entry> entries = this.anomalies.read(AnomalyDetectionFlowlet.ANOMALY_KEY, startTs, endTs);
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

  private List<Anomaly> getAnomalies(List<TimeseriesTable.Entry> entries) {
    List<Anomaly> facts = Lists.newArrayList();
    for (TimeseriesTable.Entry entry : entries) {
      Fact fact = GSON.fromJson(Bytes.toString(entry.getTags()[0]), Fact.class);
      String key = Bytes.toStringBinary(entry.getValue());
      facts.add(new Anomaly(key, fact));
    }
    return facts;
  }

  private List<Anomaly> getAnomalies(List<TimeseriesTable.Entry> entries, String groupFor) {
    // map of ts -> anomaly group -> anomaly details
    // NOTE: has to bee sorted map to keep order by time
    Map<Long, Map<String, Anomaly>> facts = Maps.newTreeMap();
    for (TimeseriesTable.Entry entry : entries) {
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
