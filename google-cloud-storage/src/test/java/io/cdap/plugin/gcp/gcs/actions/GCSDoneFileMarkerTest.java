/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs.actions;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.batch.action.Condition;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link GCSDoneFileMarker}
 */
public class GCSDoneFileMarkerTest {

  private GCSDoneFileMarker.Config getValidConfig() {
    return GCSDoneFileMarker.Config.builder()
      .setProject("test")
      .setGcsPath("gs://test")
      .setServiceAccountType("filePath")
      .setServiceFilePath("/service-account.json")
      .setRunCondition(Condition.SUCCESS.name())
      .build();
  }

  private GCSDoneFileMarker.Config getInvalidConfig() {
    return GCSDoneFileMarker.Config.builder()
      .setProject("test")
      .setGcsPath("sg:/test")
      .setServiceAccountType("filePath")
      .setServiceFilePath("auto-detect")
      .setRunCondition(Condition.SUCCESS.name())
      .build();
  }

  @Test
  public void testValidFSProperties() {
    GCSDoneFileMarker.Config config = getValidConfig();
    MockFailureCollector collector = new MockFailureCollector("gcs_done_file_marker_collector_one");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testInvalidFSProperties() throws NoSuchFieldException {
    GCSDoneFileMarker.Config config = getInvalidConfig();
    MockFailureCollector collector = new MockFailureCollector("gcs_done_file_marker_collector_two");
    ValidationFailure failure = null;
    try {
      config.validate(collector);
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      failure = e.getFailures().get(0);
    }
    Assert.assertEquals(GCSDoneFileMarker.Config.NAME_PATH,
                        failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
