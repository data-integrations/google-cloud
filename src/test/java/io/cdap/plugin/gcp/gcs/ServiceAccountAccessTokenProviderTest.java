/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs;

import com.google.api.client.auth.oauth2.Credential;
import com.google.auth.oauth2.AccessToken;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.CredentialFromAccessTokenProviderClassFactory;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.common.collect.ImmutableList;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.common.ServerErrorException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

/**
 * Unit Tests for {@link ServiceAccountAccessTokenProvider}.
 */
public class ServiceAccountAccessTokenProviderTest {

  @Test
  public void testServiceAccountAccessTokenProviderIsUsed() throws IOException {

    Map<String, String> authProperties = GCPUtils.generateGCSAuthProperties(null,
        "filePath");
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> prop : authProperties.entrySet()) {
      conf.set(prop.getKey(), prop.getValue());
    }

    AccessTokenProvider accessTokenProvider =
        HadoopCredentialConfiguration.getAccessTokenProvider(conf, ImmutableList.of(
        GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX));

    Assert.assertTrue(String.format("AccessTokenProvider should be an instance of %s",
            ServiceAccountAccessTokenProvider.class.getName()),
        accessTokenProvider instanceof ServiceAccountAccessTokenProvider);
  }

  @Test
  public void testServiceAccountAccessTokenProvider() throws IOException {
    Map<String, String> authProperties = GCPUtils.generateGCSAuthProperties(null,
        "filePath");
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> prop : authProperties.entrySet()) {
      conf.set(prop.getKey(), prop.getValue());
    }
    // {@link CredentialFromAccessTokenProviderClassFactory#credential} does not propagate the
    // config to {@link ServiceAccountAccessTokenProvider} which should not cause NPE
    Credential credential = CredentialFromAccessTokenProviderClassFactory.credential(
        conf, ImmutableList.of(GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX),
        CredentialFactory.DEFAULT_SCOPES
    );
    Assert.assertNotNull(credential);
  }

  @Test
  public void testIsServerErrorWith5xx() {
    IOException serverError = new IOException(
      "Unexpected Error code 500 trying to get security access token from Compute Engine metadata for the default " +
        "service account.");
    Assert.assertTrue(ServiceAccountAccessTokenProvider.isServerError(serverError));
  }

  @Test
  public void testIsServerErrorWithNon5xxErrorCode400() {
    IOException clientError = new IOException(
      "Unexpected Error code 400 trying to get security access token from Compute Engine metadata for the default " +
        "service account.");
    Assert.assertFalse(ServiceAccountAccessTokenProvider.isServerError(clientError));
  }

  @Test
  public void testIsServerErrorWithNon5xxErrorCode403() {
    IOException forbiddenError = new IOException(
      "Unexpected Error code 403 trying to get security access token from Compute Engine metadata for the default " +
        "service account.");
    Assert.assertFalse(ServiceAccountAccessTokenProvider.isServerError(forbiddenError));
  }

  @Test
  public void testIsServerErrorWith5xxErrorCode503() {
    IOException serverError = new IOException(
      "Unexpected Error code 503 trying to get security access token from Compute Engine metadata for the default " +
        "service account.");
    Assert.assertTrue(ServiceAccountAccessTokenProvider.isServerError(serverError));
  }

  @Test(expected = ServerErrorException.class)
  public void testRetryMechanismFailsAfterMaxRetries() throws IOException {
    ServiceAccountAccessTokenProvider provider = Mockito.spy(new ServiceAccountAccessTokenProvider());
    Mockito.doThrow(new ServerErrorException(503, "Unexpected Error code 503 trying to get security access token " +
        "from Compute Engine metadata for the default service account.", null))
      .when(provider).retrieveAccessToken();
    provider.getAccessToken();
  }

  @Test
  public void testRetryMechanismSucceedsAfterFewRetries() throws IOException {
    ServiceAccountAccessTokenProvider provider = Mockito.spy(new ServiceAccountAccessTokenProvider());

    // Create a valid token with future expiration
    AccessToken validToken = new AccessToken("valid-token", Date.from(Instant.now().plusSeconds(3600)));

    // Fail first 2 attempts, then succeed
    Mockito.doThrow(new ServerErrorException(503, "Unexpected Error code 503 trying to get security access token " +
        "from Compute Engine metadata for the default service account.", null))
           .doThrow(new ServerErrorException(500, "Unexpected Error code 500 trying to get security access token " +
        "from Compute Engine metadata for the default service account.", null))
      .doReturn(validToken)
      .when(provider).retrieveAccessToken();
    AccessTokenProvider.AccessToken accessToken = provider.getAccessToken();

    Assert.assertNotNull(accessToken);
    Assert.assertEquals("valid-token", accessToken.getToken());

    // Verify that retrieveAccessToken was called 3 times (2 failures + 1 success)
    Mockito.verify(provider, Mockito.times(3)).retrieveAccessToken();
  }
}
