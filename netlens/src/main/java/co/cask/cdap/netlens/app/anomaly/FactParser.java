package co.cask.cdap.netlens.app.anomaly;

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import com.google.common.base.Charsets;
import com.google.common.io.LineReader;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.StringReader;
import java.util.Map;

/**
 * Parses stream data, outputs {@link Fact}s.
 *
 * src: source ip
 * spt: source port
 * dst: destination ip
 * dpt: destination port
 * app: traffic type, e.g. TCP
 * rt: request time - long
 * rs: request size (missing from our logs) - int
 * rl: latency (missing from our logs) - int
 * ipv: internet protocol e.g. IPv4
 *
 * atz: source timezone
 * dtz: destination timezone
 */
public class FactParser extends AbstractFlowlet {
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

  private Fact parseStream(String line) {
    Map<String, String> map = GSON.fromJson(line, new TypeToken<Map<String, String>>(){}.getType());
    return new Fact(System.currentTimeMillis(), map);
  }
}
