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
    String validServiceAccountJson = "{\n" +
        "  \"type\": \"service_account\",\n" +
        "  \"project_id\": \"test-project\",\n" +
        "  \"private_key_id\": \"dummy-id\",\n" +
        "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCnf55gHwg2wuFS\\nX9L29N0txE1mKGCIXT+jdCamShzelIanidPhlMmckD5r8T2oue3r0TCXLvO+KEIc\\nNZg4Uw34YBpmBgXIMrmqXj3qGTaFJ0DoDDGAzocdnMQ+V4uUs8Pgudw370mHbRdI\\nZhA4ejxVACq2M/sWU1GJNLEVjN6/EevhaaoZr/fyxJIAoAOaS63wzzS/0AQPMkpP\\nxpkKyVgJLmSg0U5Uw8wAAkefS+anIa0lYQkH/SMkW7VhtlSFJQuPUOXMGJwVEGXy\\nZ+mDIfiljGUtk0EqsCarhI1sxa9khVGGLUo8qBXDk0iP0GlEomsD2scB7uyOJWaI\\nKrrsIQI7AgMBAAECggEAJjq8fRfYGheUseZpEjCFIuMA/2YL0lPmKHpkL+QOfsbL\\njQWqNHxvH6rUdHKVLiNxqDcQrhDrVOV/YUziN0jNkXjtzYdmXnElazsjSoECrpVs\\n/Ql7avi4YgvbxpbYptH4zRvepW2aDKzbeYzB+wH7LzNgjEGE922A2J4MJO0IEfe4\\nlbzjFtvznnZIisnoqtnnCrNRpKMzePyk2ABY3dw1nPQPYytL74Cusz7Sji400XMZ\\n8NrgOCPKpJ7aiTBsqxk6YFtJQymJl6qo6XkT3p0E1X4iEBMB447Rid5Xo7Ham+tA\\nCsTcE7pmQmjB+SvqaIYR/yyDBsjIjcnhRQWXV65uQQKBgQDW2PtocL8J4X9Fr8NA\\n7aPyFJQZ9QFCuYsTwWpUCJruXWcGf+qyQMygQeiLFZjOHOylsN2VyeWrpeedZ4fw\\nlKuQm+/eaCuwl46ne/PTL4XuyiapVRyIg1Fv7z5uIok9CuH7/umqO6U55df/JGqR\\nkyXi5D5g3poyr/rKW2keENjpMQKBgQDHlOHMzk00IKQqCLvXXr1nGTiZnK+938zV\\neJrB1rnfF2ahVWWL+gJcPx5Tm7mvXAiKqAOwQXrsYSl0SulRf3xdULpOGaJsncfV\\n34mGo0ejiYn0slkuodAP5AQaQfYZvBVi6WI+4r6SZYBWbYWdSnLNPsAzAvzWO+mo\\nwXejIgmHKwKBgAb5DdfK6PhaIDZTyQN/cvW1Y0UgZYUT3oaqnVfS26xmzaQxXJ2i\\nasG8wd5Zuhbea3PJNLfa0KBshwLdzFSrjpHn7bYLLXfSw2d+J2CwRymD2BNg1sc7\\nc9YtLpqGdmvLwZ/bGxxSST+CzSrTtL26x90ASWH9d4WznnEY3GWEaHXRAoGBAK0Z\\ncvIYKBvDB78xTgIQuINX+6A3prEvD1BCxy7B1vooKKpbD7TlBPEPCXWIhfcTApGI\\nAZL4Z+3mo3aqlkxKHwosPzoHv7km67CSrYvpx/OOEen6WaE5VOTDZc+EUDenyoC8\\nXKqgLJt6j2HmodF3RbS/7SERQUtqMVFdYy9JESx7AoGBAJcg4z3K+Mvs47w5cHeo\\ncPhJaoINefqnaTUPy1mr8oa0JLMI93Lleg5Qsq/WADDmObT4b6/4toRrg/WE3s5R\\nEUtgI3cFwREhaix6zzc23fyv4cjrWxrl/a7xCaUoH/gaE1Iu9zSHpQWH7wbkPj9o\\nHWa3EtRumddQwC4LkRUqNPon\\n-----END PRIVATE KEY-----\\n\",\n"
        +
        "  \"client_email\": \"test@test-project.iam.gserviceaccount.com\",\n" +
        "  \"client_id\": \"12345\"\n" +
        "}";

    GoogleCredentials creds = GCPUtils.loadServiceAccountCredentials(validServiceAccountJson, false);
    Assert.assertNotNull(creds);
  }
}
