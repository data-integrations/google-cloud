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
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.CredentialFromAccessTokenProviderClassFactory;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.common.collect.ImmutableList;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
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
}
