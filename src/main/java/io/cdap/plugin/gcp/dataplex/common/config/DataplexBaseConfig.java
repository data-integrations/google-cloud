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

package io.cdap.plugin.gcp.dataplex.common.config;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;

/**
 * Contains common Dataplex config properties.
 */
public class DataplexBaseConfig extends PluginConfig {
  protected static final String REFERENCE_NAME = "referenceName";
  protected static final String NAME_LAKE = "lake";
  protected static final String NAME_ZONE = "zone";
  protected static final String NAME_LOCATION = "location";
  protected static final String NAME_CONNECTION = "connection";
  private static final Logger LOG = LoggerFactory.getLogger(DataplexBaseConfig.class);

  @Name(REFERENCE_NAME)
  @Nullable
  @Description("Name used to uniquely identify this plugin for lineage, annotating metadata, etc.")
  protected String referenceName;

  @Name(NAME_LOCATION)
  @Macro
  @Description("ID of the location in which the Dataplex lake has been created, which can be found on the " +
    "details page of the lake.")
  protected String location;

  @Name(NAME_LAKE)
  @Macro
  @Description("ID of the Dataplex lake, which can be found on the details page of the lake.")
  protected String lake;

  @Name(NAME_ZONE)
  @Macro
  @Description("ID of the Dataplex zone, which can be found on the details page of the zone.")
  protected String zone;

  @Name(NAME_CONNECTION)
  @Nullable
  @Macro
  @Description("The existing connection to use.")
  protected GCPConnectorConfig connection;

  /**
   * Return reference name if provided, otherwise, normalize the FQN and return it as reference name
   * @return referenceName (if provided)/normalized FQN
   */
  public String getReferenceName(String fqn) {
    return Strings.isNullOrEmpty(referenceName) ? ReferenceNames.normalizeFqn(fqn) : referenceName;
  }

  public String getLake() {
    return lake;
  }

  public String getZone() {
    return zone;
  }

  public String getLocation() {
    return location;
  }

  /**
   * Return true if the service account is set to auto-detect but it can't be fetched from the environment.
   * This shouldn't result in a deployment failure, as the credential could be detected at runtime if the pipeline
   * runs on dataproc. This should primarily be used to check whether certain validation logic should be skipped.
   *
   * @return true if the service account is set to auto-detect but it can't be fetched from the environment.
   */
  public boolean autoServiceAccountUnavailable() {
    if (connection == null || connection.getServiceAccountFilePath() == null &&
      connection.isServiceAccountFilePath()) {
      try {
        ServiceAccountCredentials.getApplicationDefault();
      } catch (IOException e) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param failureCollector failureCollector to collect the exceptions and failures.
   * @return GoogleCredentials based on service account
   */
  @Nullable
  public GoogleCredentials getCredentials(FailureCollector failureCollector) {
    GoogleCredentials credentials = null;
    // validate service account
    if (connection.isServiceAccountJson() || connection.getServiceAccountFilePath() != null) {
      credentials =
        (GoogleCredentials) connection.getCredentials(failureCollector);
      return credentials;
    }
    try {
      credentials = ServiceAccountCredentials.getApplicationDefault();
    } catch (IOException e) {
      failureCollector.addFailure(e.getMessage(), null);
    }
    return credentials;
  }

  /**
   * @return Project ID
   */
  public String getProject() {
    if (connection == null) {
      throw new IllegalArgumentException(
        "Could not get project information, connection should not be null!");
    }
    return connection.getProject();
  }

  public GCPConnectorConfig getConnection() {
    return connection;
  }

  @Nullable
  public String tryGetProject() {
    return connection == null ? null : connection.tryGetProject();
  }

  @Nullable
  public String getServiceAccount() {
    return connection == null ? null : connection.getServiceAccount();
  }

  @Nullable
  public String getServiceAccountType() {
    return connection == null ? null : connection.getServiceAccountType();
  }

  @Nullable
  public Boolean isServiceAccountFilePath() {
    return connection == null ? null : connection.isServiceAccountFilePath();
  }

  /**
   * Check if service account is a valid one and returns credentials.
   *
   * @param failureCollector
   * @return credentials
   */
  public GoogleCredentials validateAndGetServiceAccountCredentials(FailureCollector failureCollector) {
    GoogleCredentials credentials = null;
    credentials = getCredentials(failureCollector);
    if (credentials == null) {
      failureCollector.addFailure(String.format("Unable to load credentials from %s.",
            connection.isServiceAccountFilePath() ? connection.getServiceAccountFilePath() : "provided JSON key"),
          "Ensure the service account file is available on the local filesystem.")
        .withConfigProperty("serviceFilePath")
        .withConfigProperty("serviceAccountJSON");
    }
    return credentials;
  }

  protected void configureDataplexException(String dataplexConfigProperty, String dataplexConfigPropType,
                                            ApiException e,
                                            FailureCollector failureCollector) {
    if (StatusCode.Code.NOT_FOUND.equals(e.getStatusCode().getCode()) ||
      StatusCode.Code.INVALID_ARGUMENT.equals(e.getStatusCode().getCode())) {
      failureCollector
        .addFailure(String.format("'%s' could not be found. Please ensure that it exists in Dataplex.",
          dataplexConfigProperty), null).withConfigProperty(dataplexConfigPropType);
      return;
    }
    if (StatusCode.Code.PERMISSION_DENIED.equals(e.getStatusCode().getCode()) ||
      StatusCode.Code.UNAUTHENTICATED.equals(e.getStatusCode().getCode())) {
      failureCollector
        .addFailure(String.format("'%s' could not be accessed. Please ensure that you have required permissions.",
          dataplexConfigProperty), null).withConfigProperty(dataplexConfigPropType);
      return;
    }
    failureCollector.addFailure(e.getCause().getMessage(), null)
      .withConfigProperty(dataplexConfigPropType);
  }

  @Nullable
  public String getServiceAccountFilePath() {
    return connection == null ? null : connection.getServiceAccountFilePath();
  }

  /**
   * Service account email is required to create task in dataplex.
   *
   * @return service account email ID
   * @throws IOException
   */
  public String getServiceAccountEmail() throws IOException {
    if (connection.isServiceAccountJson() || connection.getServiceAccountFilePath() != null) {
      try (InputStream inputStream = GCPUtils.openServiceAccount(getServiceAccount(), isServiceAccountFilePath())) {
        ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(inputStream);
        LOG.info("Using Service Account: " + credentials.getClientEmail());
        return credentials.getClientEmail();
      }
    }
    ComputeEngineCredentials credentials = (ComputeEngineCredentials) ServiceAccountCredentials.
      getApplicationDefault();
    LOG.info("Using Service Account: " + credentials.getAccount());
    // Adding this code for preview as dataplex doesn't allow user to act as Cloud Data Fusion API Service Agent and
    // create tasks in dataplex. So we are passing default Compute Engine service account in task creation.
    // This code will fetch the project number from service Agent and form compute service account email.
    // For more info https://cloud.google.com/data-fusion/docs/concepts/service-accounts
    if (credentials.getAccount().startsWith("service-")) {
      return credentials.getAccount().substring(8, credentials.getAccount().indexOf("@"))
        + "-compute@developer.gserviceaccount.com";
    }
    return credentials.getAccount();
  }
}
