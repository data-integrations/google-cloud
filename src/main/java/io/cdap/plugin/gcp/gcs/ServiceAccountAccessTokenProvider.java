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
import com.google.cloud.hadoop.util.AccessTokenProvider;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;

/**
 * An AccessTokenProvider that uses the newer GoogleCredentials library to get the credentials. This is used instead
 * of the default GCS implementation that uses the older GoogleCredential library, which does not work with external
 * service accounts.
 */
public class ServiceAccountAccessTokenProvider implements AccessTokenProvider {
  private Configuration conf;
  private GoogleCredentials credentials;

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
