package co.cask.cdap.netlens.app;

import java.util.concurrent.TimeUnit;

/**
 * Configuration constants
 * todo: should be configurable (not hardcoded)
 */
public class Constants {
  public static final long AGG_INTERVAL_SIZE = TimeUnit.SECONDS.toMillis(5);
  public static final long TOPN_AGG_INTERVAL_SIZE = TimeUnit.MINUTES.toMillis(1);
}
