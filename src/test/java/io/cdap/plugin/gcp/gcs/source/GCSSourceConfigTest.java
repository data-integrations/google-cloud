/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs.source;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;
import io.cdap.plugin.gcp.gcs.source.GCSSource.GCSSourceConfig;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Tests for {@link GCSSourceConfig} validation.
 */
public class GCSSourceConfigTest {

  private static final String FS_PROPERTIES = "fileSystemProperties";

  private GCSSourceConfig getConfig(@Nullable String fileSystemProperties) throws Exception {
    GCSSourceConfig config = new GCSSourceConfig();
    setField(config, AbstractFileSourceConfig.class, "referenceName", "testref");
    setField(config, GCSSourceConfig.class, "path", "gs://test/path");
    setField(config, GCSSourceConfig.class, "fileSystemProperties", fileSystemProperties);
    return config;
  }

  private static void setField(Object target, Class<?> clz, String name, @Nullable Object value) throws Exception {
    Field field = clz.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  @Test
  public void testNoFSProperties() throws Exception {
    MockFailureCollector collector = new MockFailureCollector("gcssource");
    getConfig(null).validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testAllowedFSProperties() throws Exception {
    MockFailureCollector collector = new MockFailureCollector("gcssource");
    getConfig("{\"fs.gs.inputstream.fadvise\":\"AUTO\",\"mapreduce.input.fileinputformat.split.minsize\":\"1\"}")
      .validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testInvalidJsonFSProperties() throws Exception {
    MockFailureCollector collector = new MockFailureCollector("gcssource");
    getConfig("{\"key\":}").validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    assertFailureOnFSProperties(collector.getValidationFailures().get(0));
  }

  @Test
  public void testReservedStorageRootUrlIsRejected() throws Exception {
    assertSingleReservedKeyFailure("fs.gs.storage.root.url");
  }

  @Test
  public void testReservedTokenServerUrlIsRejected() throws Exception {
    assertSingleReservedKeyFailure("fs.gs.token.server.url");
  }

  @Test
  public void testReservedKeyThroughFallbackPrefixIsRejected() throws Exception {
    assertSingleReservedKeyFailure("google.cloud.token.server.url");
  }

  @Test
  public void testReservedPluginAuthKeyIsRejected() throws Exception {
    assertSingleReservedKeyFailure("cdap.auth.service.account");
  }

  @Test
  public void testReservedImpersonationKeyIsRejected() throws Exception {
    assertSingleReservedKeyFailure("fs.gs.auth.impersonation.service.account");
  }

  @Test
  public void testEveryReservedKeyIsReported() throws Exception {
    MockFailureCollector collector = new MockFailureCollector("gcssource");
    getConfig("{\"fs.gs.storage.root.url\":\"https://evil.example.com/\","
                + "\"fs.gs.proxy.address\":\"evil.example.com:8080\","
                + "\"fs.gs.inputstream.fadvise\":\"AUTO\"}").validate(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    for (ValidationFailure failure : failures) {
      assertFailureOnFSProperties(failure);
    }
  }

  private void assertSingleReservedKeyFailure(String key) throws Exception {
    MockFailureCollector collector = new MockFailureCollector("gcssource");
    getConfig("{\"" + key + "\":\"https://evil.example.com/\"}").validate(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(1, failures.size());
    assertFailureOnFSProperties(failures.get(0));
    Assert.assertTrue(failures.get(0).getMessage().contains(key));
  }

  private void assertFailureOnFSProperties(ValidationFailure failure) {
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals(FS_PROPERTIES, causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
