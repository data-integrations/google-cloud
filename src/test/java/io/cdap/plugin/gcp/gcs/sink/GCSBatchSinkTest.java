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

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.List;

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

  @Test
  public void testValidContentType() throws Exception {
    GCSBatchSink.GCSBatchSinkConfig config = getConfig(null);
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    config.validate(collector);
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("format"),
                "csv");
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("contentType"),
                "application/csv");
    config.validate(collector);
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("format"),
                "tsv");
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("contentType"),
                "text/tab-separated-values");
    config.validate(collector);
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("format"),
                "json");
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("contentType"),
                "other");
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("customContentType"),
                "application/javascript");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testInvalidContentType() throws Exception {
    GCSBatchSink.GCSBatchSinkConfig config = getConfig(null);
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("format"),
                "avro");
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("contentType"),
                "text/plain");
    config.validate(collector);
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("format"),
                "csv");
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("contentType"),
                "application/avro");
    config.validate(collector);
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("format"),
                "json");
    FieldSetter
      .setField(config, GCSBatchSink.GCSBatchSinkConfig.class.getDeclaredField("contentType"),
                "text/tab-separated-values");
    config.validate(collector);
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("contentType", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    failure = collector.getValidationFailures().get(1);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("contentType", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    failure = collector.getValidationFailures().get(2);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("contentType", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
