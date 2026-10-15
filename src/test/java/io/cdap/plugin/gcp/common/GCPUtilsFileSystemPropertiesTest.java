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

package io.cdap.plugin.gcp.common;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the handling of reserved keys in user-supplied file system properties.
 */
public class GCPUtilsFileSystemPropertiesTest {

  private static final String CONFIG_PROPERTY = "fileSystemProperties";

  private static final List<String> RESERVED_KEYS = Arrays.asList(
    "fs.gs.storage.root.url",
    "fs.gs.storage.service.path",
    "fs.gs.token.server.url",
    "fs.gs.http.transport.type",
    "fs.gs.enable.service.account.auth",
    "fs.gs.service.account.auth.email",
    "fs.gs.service.account.auth.keyfile",
    "fs.gs.impl",
    "fs.AbstractFileSystem.gs.impl",
    "fs.gs.proxy.address",
    "fs.gs.proxy.username",
    "fs.gs.proxy.password",
    "fs.gs.auth.access.token.provider.impl",
    "fs.gs.auth.impersonation.service.account",
    "fs.gs.auth.service.account.json.keyfile",
    "fs.gs.auth.service.account.private.key",
    "fs.gs.auth.client.secret",
    "fs.gs.auth.refresh.token",
    "fs.gs.authorization.handler.impl",
    "fs.gs.delegation.token.binding",
    "google.cloud.token.server.url",
    "google.cloud.auth.access.token.provider.impl",
    "google.cloud.proxy.address",
    "cdap.auth.service.account",
    "cdap.auth.service.account.type",
    "cdap.auth.service.account.scopes");

  private static final List<String> ALLOWED_KEYS = Arrays.asList(
    "fs.gs.inputstream.fadvise",
    "fs.gs.outputstream.upload.chunk.size",
    "fs.gs.block.size",
    "fs.gs.http.max.retry",
    "fs.gs.http.connect-timeout",
    "fs.gs.status.parallel.enable",
    "fs.gs.project.id",
    "fs.gs.system.bucket",
    "mapreduce.input.fileinputformat.split.minsize",
    "avro.mo.config.namedOutput");

  @Test
  public void testReservedKeysAreDetected() {
    for (String key : RESERVED_KEYS) {
      Assert.assertTrue(key + " should be reserved", GCPUtils.isReservedFileSystemProperty(key));
    }
  }

  @Test
  public void testAllowedKeysAreNotDetected() {
    for (String key : ALLOWED_KEYS) {
      Assert.assertFalse(key + " should not be reserved", GCPUtils.isReservedFileSystemProperty(key));
    }
    Assert.assertFalse(GCPUtils.isReservedFileSystemProperty(null));
    Assert.assertFalse(GCPUtils.isReservedFileSystemProperty(""));
  }

  @Test
  public void testSurroundingWhitespaceDoesNotBypassReservedKeys() {
    Assert.assertTrue(GCPUtils.isReservedFileSystemProperty(" fs.gs.storage.root.url "));
    Assert.assertTrue(GCPUtils.isReservedFileSystemProperty("\tfs.gs.token.server.url"));
  }

  @Test
  public void testValidateReportsOneFailurePerReservedKey() {
    Map<String, String> properties = new HashMap<>();
    properties.put("fs.gs.storage.root.url", "https://attacker.example.com/");
    properties.put("fs.gs.token.server.url", "https://attacker.example.com/token");
    properties.put("fs.gs.inputstream.fadvise", "AUTO");

    MockFailureCollector collector = new MockFailureCollector("gcs");
    GCPUtils.validateFileSystemProperties(properties, CONFIG_PROPERTY, collector);

    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
    for (ValidationFailure failure : failures) {
      Assert.assertEquals(1, failure.getCauses().size());
      Assert.assertEquals(CONFIG_PROPERTY, failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertTrue(failure.getMessage().contains("is reserved and cannot be overridden"));
    }
  }

  @Test
  public void testValidateAcceptsAllowedKeys() {
    Map<String, String> properties = new HashMap<>();
    for (String key : ALLOWED_KEYS) {
      properties.put(key, "value");
    }
    MockFailureCollector collector = new MockFailureCollector("gcs");
    GCPUtils.validateFileSystemProperties(properties, CONFIG_PROPERTY, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateAcceptsNullAndEmptyProperties() {
    MockFailureCollector collector = new MockFailureCollector("gcs");
    GCPUtils.validateFileSystemProperties(null, CONFIG_PROPERTY, collector);
    GCPUtils.validateFileSystemProperties(new HashMap<>(), CONFIG_PROPERTY, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGetFileSystemPropertiesDropsReservedUserKeys() {
    Map<String, String> userProperties = new HashMap<>();
    userProperties.put("fs.gs.storage.root.url", "https://attacker.example.com/");
    userProperties.put("google.cloud.token.server.url", "https://attacker.example.com/token");
    userProperties.put("cdap.auth.service.account", "{\"type\":\"external_account\"}");
    userProperties.put("fs.gs.auth.access.token.provider.impl", "com.example.EvilProvider");
    userProperties.put("fs.gs.inputstream.fadvise", "AUTO");

    GCPConnectorConfig connection = new GCPConnectorConfig("my-project", "filePath", "auto-detect", null);
    Map<String, String> properties = GCPUtils.getFileSystemProperties(connection, "gs://bucket/path",
                                                                      userProperties);

    Assert.assertFalse(properties.containsKey("fs.gs.storage.root.url"));
    Assert.assertFalse(properties.containsKey("google.cloud.token.server.url"));
    Assert.assertFalse(properties.containsKey("cdap.auth.service.account"));
    // the access token provider is always the one set by the plugin
    Assert.assertEquals("io.cdap.plugin.gcp.gcs.ServiceAccountAccessTokenProvider",
                        properties.get("fs.gs.auth.access.token.provider.impl"));
    Assert.assertEquals("AUTO", properties.get("fs.gs.inputstream.fadvise"));
    Assert.assertEquals("my-project", properties.get("fs.gs.project.id"));
    Assert.assertEquals("bucket", properties.get("fs.gs.system.bucket"));
  }
}
