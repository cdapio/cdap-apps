package co.cask.cdap.netlens.app.anomaly;

import co.cask.cdap.netlens.app.anomaly.AnomalyDetectionFlowlet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class AnomalyDetectionFlowletTest {
  @Test
  public void testIsLastPointAnomaly() {
    double sensitivity = 1.0;
    double meanThreshold = 1.0;
    Assert.assertFalse(AnomalyDetectionFlowlet.isLastPointAnomaly(new int[]{0, 1, 2, 3, 4, 5, 6, 5, 4, 3, 4},
                                                                  meanThreshold, sensitivity));

    Assert.assertTrue(AnomalyDetectionFlowlet.isLastPointAnomaly(new int[]{0, 1, 2, 3, 4, 5, 6, 5, 4, 3, 24},
                                                                 meanThreshold, sensitivity));

    Assert.assertFalse(AnomalyDetectionFlowlet.isLastPointAnomaly(new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2},
                                                                  meanThreshold, sensitivity));

    Assert.assertTrue(AnomalyDetectionFlowlet.isLastPointAnomaly(new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12},
                                                                 meanThreshold, sensitivity));
  }
}
