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
import com.google.common.annotations.VisibleForTesting;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.plugin.gcp.common.GCPErrorDetailsProviderUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.common.ServerErrorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.regex.Pattern;
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
  private static final Logger LOG = LoggerFactory.getLogger(ServiceAccountAccessTokenProvider.class);
  public static final int DEFAULT_INITIAL_RETRY_DURATION_SECONDS = 5;
  public static final int DEFAULT_MAX_RETRY_COUNT = 5;
  public static final int DEFAULT_MAX_RETRY_DURATION_SECONDS = 80;
  private static final RetryPolicy<Object> RETRY_POLICY = createRetryPolicy();
  private static final Pattern SERVER_ERROR_PATTERN = Pattern.compile("Unexpected Error code 5\\d{2} trying to get " +
  "security access token from Compute Engine metadata for the default service account.*");

  @VisibleForTesting
  @Override
  public AccessToken getAccessToken() {
      try {
        return Failsafe.with(RETRY_POLICY).get(() -> {
            com.google.auth.oauth2.AccessToken token = retrieveAccessToken();
            if (token == null || token.getExpirationTime().before(Date.from(Instant.now()))) {
              refresh();
              token = retrieveAccessToken();
            }
            return new AccessToken(token.getTokenValue(), token.getExpirationTime().getTime());
          });
      } catch (FailsafeException e) {
        Throwable t = e.getCause() != null ? e.getCause() : e;
        ErrorType errorType = (t instanceof ServerErrorException) ? ErrorType.SYSTEM : ErrorType.UNKNOWN;
        throw GCPErrorDetailsProviderUtil.getHttpResponseExceptionDetailsFromChain(
          e, "Unable to get service account access token after retries.", errorType, true,
          GCPUtils.GCE_METADATA_SERVER_ERROR_SUPPORTED_DOC_URL
        );
      }
    }

  private static RetryPolicy<Object> createRetryPolicy() {
    return RetryPolicy.builder()
      .handle(ServerErrorException.class)
      .withBackoff(Duration.ofSeconds(DEFAULT_INITIAL_RETRY_DURATION_SECONDS),
                   Duration.ofSeconds(DEFAULT_MAX_RETRY_DURATION_SECONDS))
      .withMaxRetries(DEFAULT_MAX_RETRY_COUNT)
      .onRetry(event -> LOG.debug("Retry attempt {} due to {}", event.getAttemptCount(), event.getLastException().
        getMessage()))
      .onSuccess(event -> LOG.debug("Access Token Fetched Successfully."))
      .onRetriesExceeded(
       event -> LOG.error("Unable to get service account access token after {} retries.", event.getAttemptCount() - 1))
      .build();
  }

  @VisibleForTesting
  static boolean isServerError(IOException e) {
    String msg = e.getMessage();
    return msg != null && SERVER_ERROR_PATTERN.matcher(msg).matches();
  }

  com.google.auth.oauth2.AccessToken retrieveAccessToken() throws IOException {
    try {
      return getCredentials().getAccessToken();
    } catch (IOException e) {
      if (isServerError(e)) {
        throw new ServerErrorException(HttpStatus.SC_SERVICE_UNAVAILABLE, "Server error while fetching access token: "
          + e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public void refresh() throws IOException {
    try {
      getCredentials().refresh();
    } catch (IOException e) {
      if (isServerError(e)) {
        throw new ServerErrorException(HttpStatus.SC_SERVICE_UNAVAILABLE, "Server error during refresh: " +
          e.getMessage(), e);
      }
      throw GCPErrorDetailsProviderUtil.getHttpResponseExceptionDetailsFromChain(
        e, "Unable to refresh service account access token.", ErrorType.UNKNOWN, true,
        GCPUtils.GCE_METADATA_SERVER_ERROR_SUPPORTED_DOC_URL);
    }
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
