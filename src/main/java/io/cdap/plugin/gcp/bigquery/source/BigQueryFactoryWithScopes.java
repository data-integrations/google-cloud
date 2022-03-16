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
import com.google.cloud.hadoop.io.bigquery.BigQueryFactory;
import com.google.cloud.hadoop.util.AccessTokenProviderClassFromConfigFactory;
import com.google.cloud.hadoop.util.CredentialFromAccessTokenProviderClassFactory;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import io.cdap.plugin.gcp.common.GCPUtils;
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
      CredentialFromAccessTokenProviderClassFactory.credential(
        new AccessTokenProviderClassFromConfigFactory().withOverridePrefix("mapred.bq"),
        config,
        scopes);
    if (credential != null) {
      return credential;
    }

    return HadoopCredentialConfiguration.newBuilder()
      .withConfiguration(config)
      .withOverridePrefix(BIGQUERY_CONFIG_PREFIX)
      .build()
      .getCredential(scopes);
  }
}
