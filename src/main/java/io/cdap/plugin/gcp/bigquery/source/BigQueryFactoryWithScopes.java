/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.source;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFactory;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.CredentialFromAccessTokenProviderClassFactory;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.common.collect.ImmutableList;
import io.cdap.plugin.gcp.gcs.ServiceAccountAccessTokenProvider;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 * Override for {@link BigQueryFactory} with additional scopes for External tables.
 */
public class BigQueryFactoryWithScopes extends BigQueryFactory {

  private final List<String> scopes;

  public BigQueryFactoryWithScopes(List<String> scopes) {
    this.scopes = scopes;
  }

  @Override
  public Credential createBigQueryCredential(Configuration config) throws GeneralSecurityException, IOException {
    Credential credential =
        CredentialFromAccessTokenProviderClassFactory.credential(getAccessTokenProvider(config),
            scopes);
    if (credential != null) {
      return credential;
    }

    return HadoopCredentialConfiguration.getCredentialFactory(
            config, String.valueOf(ImmutableList.of(BigQueryConfiguration.BIGQUERY_CONFIG_PREFIX)))
        .getCredential(scopes);
  }

  /**
   * returns the {@link AccessTokenProvider} that uses the newer GoogleCredentials
   * library to get the credentials.
   *
   * @param config Hadoop {@link Configuration}
   * @return {@link ServiceAccountAccessTokenProvider}
   */
  private AccessTokenProvider getAccessTokenProvider(Configuration config) {
    AccessTokenProvider accessTokenProvider = new ServiceAccountAccessTokenProvider();
    accessTokenProvider.setConf(config);
    return accessTokenProvider;
  }
}
