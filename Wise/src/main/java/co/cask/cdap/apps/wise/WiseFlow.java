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

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.HashPartition;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;

/**
 * Flow of the Wise application. It transforms access logs to extract their information and store
 * them into the {@code pageViewCDS} Dataset.
 */
public class WiseFlow extends AbstractFlow {

  @Override
  public void configure() {
    setName("WiseFlow");
    setDescription("Wise Flow");
    addFlowlet("parser", new LogEventParserFlowlet());
    addFlowlet("pageViewCount", new PageViewCounterFlowlet());
    connectStream("logEventStream", "parser");
    connect("parser", "pageViewCount");
  }

  /**
   * Parse the IP address and the requested page URI from a log event
   * into a PageView instance and emit it to the next Flowlets.
   */
  public static class LogEventParserFlowlet extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(LogEventParserFlowlet.class);

    // Emitter for emitting a LogInfo instance to the next Flowlet
    private OutputEmitter<LogInfo> output;

    // Annotation indicates that this method can process incoming data
    @ProcessInput
    public void processFromStream(StreamEvent event) {

      // Get a log event in String format from a StreamEvent instance
      String log = Charsets.UTF_8.decode(event.getBody()).toString();

      try {
        LogInfo logInfo = LogInfo.parse(log);
        if (logInfo != null) {
          output.emit(logInfo, "ip", logInfo.getIp().hashCode());
        }
      } catch (IOException e) {
        LOG.info("Exception while processing log event {}", log, e);
      } catch (ParseException e) {
        LOG.info("Could not parse log event {}", log, e);
      }
    }
  }

  /**
   * Aggregate the counts of unique IP addresses.
   */
  public static class PageViewCounterFlowlet extends AbstractFlowlet {

    // UseDataSet annotation indicates the page-views Dataset is used in the Flowlet
    @UseDataSet("pageViewStore")
    private PageViewStore pageViewStore;

    // Batch annotation indicates processing a batch of data objects to increase throughput
    // HashPartition annotation indicates using hash partition to distribute data in multiple Flowlet instances
    // ProcessInput annotation indicates that this method can process incoming data
    @Batch(10)
    @HashPartition("ip")
    @ProcessInput
    public void count(LogInfo logInfo) {
      // Increment the count of a logInfo by 1
      pageViewStore.incrementCount(logInfo);
    }
  }
}
