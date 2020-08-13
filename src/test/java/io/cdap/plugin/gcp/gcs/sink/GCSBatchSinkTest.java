/*
 * Copyright © 2020 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.plugin.gcp.gcs.sink;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

public class GCSBatchSinkTest {

  @Test
  public void testValidFSProperties() throws NoSuchFieldException {
    GCSBatchSink.GCSBatchSinkConfig config = getConfig(null);
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private GCSBatchSink.GCSBatchSinkConfig getConfig(String fileSystemProperties) throws NoSuchFieldException {
    GCSBatchSink.GCSBatchSinkConfig gcsBatchSinkConfig = new GCSBatchSink.GCSBatchSinkConfig();
    FieldSetter
      .setField(gcsBatchSinkConfig, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("path"), "gs://test");
    FieldSetter.setField(gcsBatchSinkConfig, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("format"), "avro");
    FieldSetter
      .setField(gcsBatchSinkConfig, GCPReferenceSinkConfig.class.getDeclaredField("referenceName"), "testref");
    FieldSetter
      .setField(gcsBatchSinkConfig, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("fileSystemProperties"),
                fileSystemProperties);
    return gcsBatchSinkConfig;
  }

  @Test
  public void testValidFSProperties1() throws NoSuchFieldException {
    GCSBatchSink.GCSBatchSinkConfig config = getConfig("{\"key\":\"val\"}");
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testInvalidFSProperties() throws NoSuchFieldException {
    GCSBatchSink.GCSBatchSinkConfig config = getConfig("{\"key\":}");
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }
}
