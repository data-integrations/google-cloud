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
import io.cdap.plugin.gcp.gcs.sink.GCSBatchSink.GCSBatchSinkConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import javax.annotation.Nullable;

public class GCSBatchSinkTest {

  @Test
  public void testValidFSProperties() {
    GCSBatchSinkConfig config = getBuilder(null).build();
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private GCSBatchSinkConfig.Builder getBuilder(@Nullable String fileSystemProperties) {
    return GCSBatchSinkConfig.builder()
      .setReferenceName("testref")
      .setGcsPath("gs://test")
      .setFormat("avro")
      .setFileSystemProperties(fileSystemProperties);
  }

  @Test
  public void testValidFSProperties1() {
    GCSBatchSink.GCSBatchSinkConfig config = getBuilder("{\"key\":\"val\"}").build();
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testInvalidFSProperties() {
    GCSBatchSink.GCSBatchSinkConfig config = getBuilder("{\"key\":}").build();
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testValidContentType() {
    GCSBatchSinkConfig.Builder builder = getBuilder(null);
    GCSBatchSinkConfig config = builder.build();
    MockFailureCollector collector = new MockFailureCollector("gcssink");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());

    config = builder.setFormat("csv").setContentType("application/csv").build();
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());

    config = builder.setFormat("tsv").setContentType("text/tab-separated-values").build();
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());

    config = builder.setFormat("json").setContentType("other").setCustomContentType("application/javascript").build();
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testInvalidContentType() {
    GCSBatchSinkConfig.Builder builder = getBuilder(null);
    MockFailureCollector collector = new MockFailureCollector("gcssink");

    GCSBatchSinkConfig config = builder.setFormat("avro").setContentType("text/plain").build();
    config.validate(collector);
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("contentType", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    config = builder.setFormat("csv").setContentType("application/avro").build();
    config.validate(collector);
    failure = collector.getValidationFailures().get(1);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("contentType", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    config = builder.setFormat("json").setContentType("text/tab-separated-values").build();
    config.validate(collector);
    failure = collector.getValidationFailures().get(2);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("contentType", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
