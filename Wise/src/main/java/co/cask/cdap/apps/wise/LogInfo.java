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

import com.google.common.base.Objects;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Store Web log raw information needed in the Wise application.
 */
public class LogInfo implements WritableComparable<LogInfo> {
  static final Pattern ACCESS_LOG_PATTERN = Pattern.compile(
    //   IP       id    user      date          request     code     size    referrer    user agent
    "^([\\d.]+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]+)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"");

  // SimpleDateFormat object to parse a date of format day/month/year:hour:minute:second
  private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z");

  private String ip;
  private String uri;
  private Long timestamp;

  public LogInfo() {
    // Empty constructor used by Hadoop when deserializing a LogInfo object
  }

  public LogInfo(String ip, String uri, long timestamp) {
    this.ip = ip;
    this.uri = uri;
    this.timestamp = timestamp;
  }

  @Nullable
  public static LogInfo parse(String logLine) throws IOException, ParseException {
    Matcher matcher = ACCESS_LOG_PATTERN.matcher(logLine);
    if (!matcher.matches() || matcher.groupCount() < 8) {
      return null;
    }

    // Grab the IP address from a log event
    String ip = matcher.group(1);

    // Grab the requested page URI from the log event
    String request = matcher.group(5);
    int startIndex = request.indexOf(" ");
    int endIndex = request.indexOf(" ", startIndex + 1);
    String uri = request.substring(startIndex + 1, endIndex);

    // Grab the timestamp from the log event
    String tsStr = matcher.group(4);
    Date date = TIMESTAMP_FORMAT.parse(tsStr);
    long timestamp = date.getTime();

    return new LogInfo(ip, uri, timestamp);
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public String getUri() {
    return uri;
  }

  public String getIp() {
    return ip;
  }

  @Override
  public int compareTo(LogInfo logInfo) {
    int result = ip.compareTo(logInfo.ip);
    if(result == 0) {
      result = timestamp.compareTo(logInfo.timestamp);
    }
    return result;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeString(dataOutput, ip);
    WritableUtils.writeString(dataOutput, uri);
    dataOutput.writeLong(timestamp);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    ip = WritableUtils.readString(dataInput);
    uri = WritableUtils.readString(dataInput);
    timestamp = dataInput.readLong();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("ip", ip)
      .add("uri", uri)
      .add("timestamp", timestamp)
      .toString();
  }
}
