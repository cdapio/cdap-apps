package co.cask.cdap.apps.netlens.app.anomaly;

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.LineReader;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * send data in this format, csv of fieldname:field
 * frame.number:1.0,frame.time_delta:0.0000,frame.cap_len:66,ip.src:73.189.239.50,ip.dst:10.240.23.250,
 * tcp.srcport:56748,tcp.dstport:22,udp.srcport:,udp.dstport:,ipv6.src:,ipv6.dst:
 */
public class WireSharkFactParser extends AbstractFlowlet {
  private static final Gson GSON = new Gson();
  private OutputEmitter<Fact> output;

  @Batch(100)
  @ProcessInput
  public void processFromStream(StreamEvent event) throws Exception {
    LineReader reader = new LineReader(new StringReader(Charsets.UTF_8.decode(event.getBody()).toString()));
    String line = reader.readLine();
    while (line != null) {
      Fact fact = parseStream(line);
      output.emit(fact);
      line = reader.readLine();
    }
  }


  private Fact parseStream(String line) throws Exception {
    List<String> wireData = Lists.newArrayList();
    Iterables.addAll(wireData, Splitter.on(",").split(line));
    Map<String, String> fact = Maps.newLinkedHashMap();
    Map<String, String> wireFact = Maps.newLinkedHashMap();


    for (String field : wireData) {
      String[] fieldParts = field.split(":");

      if (fieldParts.length < 1 ) {
        throw new Exception(String.format("Invalid format for field %s in packet %s", field, line));
      }

      if (fieldParts.length == 2) {
        wireFact.put(fieldParts[0], fieldParts[1]);
      }
    }

    // IPv4 or IPv6
    if (wireFact.containsKey("ip.src") && wireFact.containsKey("ip.dst")) {
      fact.put("src", wireFact.get("ip.src"));
      fact.put("dst", wireFact.get("ip.dst"));
      fact.put("ipv", "IPv4");
      populateTimeZoneInformation(fact, wireFact.get("ip.src"), wireFact.get("ip.dst"));
    } else if (wireFact.containsKey("ipv6.src") && wireFact.containsKey("ipv6.dst")) {
      fact.put("src", wireFact.get("ipv6.src"));
      fact.put("dst", wireFact.get("ipv6.dst"));
      fact.put("ipv", "IPv6");
      populateTimeZoneInformation(fact, wireFact.get("ipv6.src"), wireFact.get("ipv6.dst"));
    }

    // TCP or UDP
    if (wireFact.containsKey("tcp.srcport") && wireFact.containsKey("tcp.dstport")) {
      fact.put("spt", wireFact.get("tcp.srcport"));
      fact.put("dpt", wireFact.get("tcp.dstport"));
      fact.put("app", "TCP");
    } else if (wireFact.containsKey("udp.srcport") && wireFact.containsKey("udp.dstport")) {
      fact.put("spt", wireFact.get("udp.srcport"));
      fact.put("dpt", wireFact.get("udp.dstport"));
      fact.put("app", "UDP");
    }

    fact.put("rs", wireFact.get("frame.cap_len"));
    fact.put("rl", wireFact.get("frame.time_delta"));

    return new Fact(System.currentTimeMillis(), fact);
  }

  protected HttpURLConnection openURL(URL path, org.jboss.netty.handler.codec.http.HttpMethod method)
    throws IOException, URISyntaxException {
    HttpURLConnection urlConn = (HttpURLConnection) path.openConnection();
    urlConn.setRequestMethod(method.getName());
    return urlConn;
  }


  private void populateTimeZoneInformation(Map<String, String> fact, String src, String dest) throws Exception {
    // NOTE: using freegeoip to get this information, MAX LIMIT is 10k per hour
    URL srcURL = new URL("http://freegeoip.net/json/" + src);
    URL destURL = new URL("http://freegeoip.net/json/" + dest);
    getCountryFromIp(fact, srcURL, "atz");
    getCountryFromIp(fact, srcURL, "dtz");
  }

  private void getCountryFromIp(Map<String, String> fact, URL url, String key) throws Exception {
    HttpURLConnection urlConn = openURL(url, org.jboss.netty.handler.codec.http.HttpMethod.GET);
    //Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    if (urlConn.getResponseCode() != 200) {
      return;
    }
    Map<String, String> result = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                                          Charsets.UTF_8),
                                               new TypeToken<Map<String, String>>() {}.getType());

    urlConn.disconnect();

    if (result.containsKey("country_name")) {
      fact.put(key, result.get("country_name"));
    }
  }
}
