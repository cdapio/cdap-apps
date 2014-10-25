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

package co.cask.cdap.apps.sentiment;

import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TweetCollector extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(TweetCollector.class);

  private OutputEmitter<Tweet> output;

  private CollectingThread collector;
  private BlockingQueue<Tweet> queue;

  private Metrics metrics;

  private TwitterStream twitterStream;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    Map<String, String> args = context.getRuntimeArguments();

    if (args.containsKey("disable.public")) {
      String publicArg = args.get("disable.public");
      LOG.info("Public Twitter source turned off (disable.public={})", publicArg);
      return;
    }

    if (!args.containsKey("oauth.consumerKey") || !args.containsKey("oauth.consumerSecret")
     || !args.containsKey("oauth.accessToken") || !args.containsKey("oauth.accessTokenSecret")) {
      final String CREDENTIALS_MISSING = "Twitter API credentials not provided in runtime arguments.";
      LOG.error(CREDENTIALS_MISSING);
    }

    queue = new LinkedBlockingQueue<Tweet>(10000);
    collector = new CollectingThread();
    collector.start();
  }

  @Override
  public void destroy() {
    if (collector != null) {
      collector.interrupt();
    }
    if (twitterStream != null) {
      twitterStream.cleanUp();
      twitterStream.shutdown();
    }
  }

  @Tick(unit = TimeUnit.MILLISECONDS, delay = 100)
  public void collect() throws InterruptedException {
    if (this.queue == null) {
      // Sleep and return if public timeline is disabled
      Thread.sleep(1000);
      return;
    }
    int batchSize = 100;

    for (int i = 0; i < batchSize; i++) {
      Tweet tweet = queue.poll();
      if (tweet == null) {
        break;
      }

      metrics.count("public.total", 1);
      output.emit(tweet);
    }
  }

  private class CollectingThread extends Thread {

    @Override
    public void run() {
      try {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(false);
        cb.setAsyncNumThreads(1);

        Map<String, String> args = getContext().getRuntimeArguments();

        // Override twitter4j.properties file, if provided in runtime args.
        if (args.containsKey("oauth.consumerKey") && args.containsKey("oauth.consumerSecret")
         && args.containsKey("oauth.accessToken") && args.containsKey("oauth.accessTokenSecret")) {
          cb.setOAuthConsumerKey(args.get("oauth.consumerKey"));
          cb.setOAuthConsumerSecret(args.get("oauth.consumerSecret"));
          cb.setOAuthAccessToken(args.get("oauth.accessToken"));
          cb.setOAuthAccessTokenSecret(args.get("oauth.accessTokenSecret"));
        }

        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        StatusListener listener = new StatusAdapter() {
          @Override
          public void onStatus(Status status) {
            String text = status.getText();
            String lang = status.getLang();
            metrics.count("lang." + lang, 1);
            if (!lang.equals("en")) {
              metrics.count("otherLang", 1);
              return;
            }
            try {
              queue.put(new Tweet(text, status.getCreatedAt().getTime()));
            } catch (InterruptedException e) {
              LOG.warn("Interrupted writing to queue", e);
              return;
            }
          }

          @Override
          public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            LOG.error("Got track limitation notice:" + numberOfLimitedStatuses);
          }

          @Override
          public void onException(Exception ex) {
            LOG.warn("Error during reading from stream" + ex.getMessage());
          }
        };

        twitterStream.addListener(listener);
        twitterStream.sample();
      } catch (Exception e) {
        LOG.error("Got exception {}", e);
      } finally {
        LOG.info("CollectingThread run() exiting");
      }
    }
  }
}
