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
import io.cdap.cdap.etl.mock.common.MockArguments;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Unit Tests for {@link CmekUtils}
 */
public class CmekUtilsTest {

  /**
   * Test to check if cmek key passed from config is given priority over cmek key passed from System Preferences
   * or Runtime Arguments.
   */
  @Test
  public void testGetCmekKey() {
    String project = UUID.randomUUID().toString();
    MockFailureCollector collector = new MockFailureCollector();
    String configKey = String.format("projects/%s/locations/key-location/keyRings/ring/cryptoKeys/test_key", project);
    String key = String.format("projects/%s/locations/us/key-location/my_ring/cryptoKeys/test_key", project);

    MockArguments arguments = new MockArguments();
    arguments.set("gcp.cmek.key.name", key);

    CryptoKeyName keyReturned = CmekUtils.getCmekKey(configKey, arguments.asMap(), collector);
    Assert.assertEquals(configKey, keyReturned.toString());
  }
}
