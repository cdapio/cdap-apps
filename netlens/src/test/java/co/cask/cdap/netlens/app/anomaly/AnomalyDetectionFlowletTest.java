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
