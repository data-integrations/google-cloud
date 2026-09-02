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

import com.google.auth.oauth2.GoogleCredentials;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;

public class GCPUtilsTest {

  @Test
  public void testLoadServiceAccountCredentialsNegative() {
    // Negative Scenario: type is set to "external_account" (simulating the external
    // payload attack)
    String externalAccountJson = "{\n" +
        "  \"type\": \"external_account\"\n" +
        "}";

    try {
      GCPUtils.loadServiceAccountCredentials(externalAccountJson, false);
      Assert.fail("Expected IOException when loading non-service-account credentials");
    } catch (IOException e) {
      // Expected exception because the JSON does not represent a valid service
      // account.
      // ServiceAccountCredentials.fromStream throws a specific exception indicating
      // the type mismatch.
      Assert.assertNotNull("Exception message should indicate valid parse failure", e.getMessage());
    }
  }

  @Test
  public void testLoadServiceAccountCredentialsPositive() throws Exception {
    java.security.KeyPairGenerator kpg = java.security.KeyPairGenerator.getInstance("RSA");
    kpg.initialize(1024);
    java.security.KeyPair kp = kpg.generateKeyPair();
    String encodedKey = java.util.Base64.getEncoder().encodeToString(kp.getPrivate().getEncoded());
    String pemKey = "-----BEGIN PRIVATE KEY-----\\n" + encodedKey + "\\n-----END PRIVATE KEY-----\\n";
    String validServiceAccountJson = "{\n" +
        "  \"type\": \"service_account\",\n" +
        "  \"project_id\": \"test-project\",\n" +
        "  \"private_key_id\": \"dummy-id\",\n" +
        "  \"private_key\": \"" + pemKey + "\",\n" +
        "  \"client_email\": \"test@test-project.iam.gserviceaccount.com\",\n" +
        "  \"client_id\": \"12345\"\n" +
        "}";

    GoogleCredentials creds = GCPUtils.loadServiceAccountCredentials(validServiceAccountJson, false);
    Assert.assertNotNull(creds);
    Assert.assertTrue("Credentials should be an instance of ServiceAccountCredentials", 
                      creds instanceof com.google.auth.oauth2.ServiceAccountCredentials);
  }
}
