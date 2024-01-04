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


package io.cdap.plugin.gcp.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.bigtable.repackaged.com.google.gson.Gson;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.CredentialFactory;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An AccessTokenProvider that uses the newer GoogleCredentials library to get the credentials. This is used instead
 * of the default GCS implementation that uses the older GoogleCredential library, which does not work with external
 * service accounts.
 */
public class ServiceAccountAccessTokenProvider implements AccessTokenProvider {
  private Configuration conf;
  private GoogleCredentials credentials;
  private static final Gson GSON = new Gson();

  @Override
  public AccessToken getAccessToken() {
    try {
      com.google.auth.oauth2.AccessToken token = getCredentials().getAccessToken();
      if (token == null || token.getExpirationTime().before(Date.from(Instant.now()))) {
        refresh();
        token = getCredentials().getAccessToken();
      }
      return new AccessToken(token.getTokenValue(), token.getExpirationTime().getTime());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void refresh() throws IOException {
    getCredentials().refresh();
  }

  private GoogleCredentials getCredentials() throws IOException {
    if (credentials == null) {
      if (conf == null) {
        // {@link CredentialFromAccessTokenProviderClassFactory#credential} does not propagate the
        // config to {@link ServiceAccountAccessTokenProvider} which causes NPE when
        // initializing {@link ForwardingBigQueryFileOutputCommitter because conf is null.
        conf = new Configuration();
        // Add scopes information which is lost when running in sandbox mode.
        conf.set(GCPUtils.SERVICE_ACCOUNT_SCOPES, GSON.toJson(
            Stream.concat(CredentialFactory.DEFAULT_SCOPES.stream(),
                GCPUtils.BIGQUERY_SCOPES.stream()).collect(Collectors.toList())));
      }
      credentials = GCPUtils.loadCredentialsFromConf(conf);
    }
    return credentials;
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
