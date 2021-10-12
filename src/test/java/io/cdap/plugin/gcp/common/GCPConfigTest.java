/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.cloud.kms.v1.CryptoKeyName;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.mock.common.MockArguments;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.UUID;

/**
 * Tests for {@link GCPConfig}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(GCPConfig.class)
public class GCPConfigTest {

  @Test
  public void testNullServiceAccountType() throws NoSuchFieldException {
    GCPConfig gcpConfig = new GCPConfig();
    FieldSetter.setField(gcpConfig, GCPConfig.class.getDeclaredField("serviceAccountType"), null);
    Assert.assertEquals(gcpConfig.getServiceAccountType(), GCPConfig.SERVICE_ACCOUNT_FILE_PATH);
  }

  @Test
  public void testIsServiceAccountFilePath() throws NoSuchFieldException {
    GCPConfig gcpConfig = new GCPConfig();
    FieldSetter.setField(gcpConfig, GCPConfig.class.getDeclaredField("serviceFilePath"), "test-path");
    Assert.assertEquals(gcpConfig.isServiceAccountFilePath(), true);
  }

  @Test
  public void testFilePathServiceAccountWithoutAccountType() throws NoSuchFieldException {
    GCPConfig gcpConfig = new GCPConfig();
    FieldSetter.setField(gcpConfig, GCPConfig.class.getDeclaredField("serviceFilePath"), "test-path");
    Assert.assertEquals(gcpConfig.getServiceAccountType(), GCPConfig.SERVICE_ACCOUNT_FILE_PATH);
  }

  @Test
  public void testGetCmekKey() {
    String project = UUID.randomUUID().toString();
    MockFailureCollector collector = new MockFailureCollector();
    String configKey = String.format("projects/%s/locations/us-east1/keyRings/my_ring/cryptoKeys/test_key", project);
    String key = String.format("projects/%s/locations/us/keyRings/my_ring/cryptoKeys/test_key", project);

    GCPConfig config = PowerMockito.mock(GCPConfig.class);
    PluginProperties.Builder builder = PluginProperties.builder();
    PluginProperties properties = builder.add("cmekKey", configKey).build();
    MockArguments arguments = new MockArguments();
    arguments.set("gcp.cmek.key.name", key);
    PowerMockito.when(config.getCmekKey(arguments, collector)).thenCallRealMethod();
    PowerMockito.when(config.getProperties()).thenReturn(properties);

    CryptoKeyName keyReturned = config.getCmekKey(arguments, collector);
    Mockito.verify(config).getProperties();
    Assert.assertEquals(configKey, keyReturned.toString());
  }
}
