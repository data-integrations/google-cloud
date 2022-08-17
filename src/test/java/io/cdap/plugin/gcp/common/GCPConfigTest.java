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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

/**
 * Tests for {@link GCPConfig}
 */
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
}
