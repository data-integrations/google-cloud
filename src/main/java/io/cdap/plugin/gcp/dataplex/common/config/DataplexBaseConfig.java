/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.exception.DataplexException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
  private static final int ERROR_CODE_NOT_FOUND = 404;
  private static final int ERROR_CODE_FORBIDDEN = 403;
  private static final int ERROR_CODE_BAD_REQUEST = 400;

  @Name(REFERENCE_NAME)
  @Description("Name used to uniquely identify this sink for lineage, annotating metadata, etc.")
  protected String referenceName;

  @Name(NAME_LOCATION)
  @Macro
  @Description("Resource id for the Dataplex location. User can type it in or press a browse button which enables" +
    " hierarchical selection.")
  protected String location;

  @Name(NAME_LAKE)
  @Macro
  @Description("Resource id for the Dataplex lake. User can type it in or press a browse button which enables " +
    "hierarchical selection.")
  protected String lake;

  @Name(NAME_ZONE)
  @Macro
  @Description("Resource id for the Dataplex zone. User can type it in or press a browse button which enables " +
    "hierarchical selection.")
  protected String zone;

  @Name(NAME_CONNECTION)
  @Nullable
  @Macro
  @Description("The existing connection to use.")
  protected GCPConnectorConfig connection;

  public String getReferenceName() {
    return referenceName;
  }

  public String getLake() {
    return lake;
  }

  public void setLake(String lake) {
    this.lake = lake;
  }

  public String getZone() {
    return zone;
  }

  public void setZone(String zone) {
    this.zone = zone;
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

  public GoogleCredentials getCredentials() {
    GoogleCredentials credentials = null;
    try {
      credentials = getCredentialsFromServiceAccount();
    } catch (IOException e) {
      LOG.debug("Unable to load service account credentials due to error: {}", e.getMessage());
    }
    return credentials;
  }

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

  public void validateServiceAccount(FailureCollector failureCollector) {
    if (connection.isServiceAccountJson() || connection.getServiceAccountFilePath() != null) {
      GoogleCredentials credentials = getCredentials();
      if (credentials == null) {
        failureCollector.addFailure(String.format("Unable to load credentials from %s.",
              connection.isServiceAccountFilePath() ? connection.getServiceAccountFilePath() : "provided JSON key"),
            "Ensure the service account file is available on the local filesystem.")
          .withConfigProperty("serviceFilePath")
          .withConfigProperty("serviceAccountJSON");
        throw failureCollector.getOrThrowException();
      }
    }
  }

  public GoogleCredentials getCredentialsFromServiceAccount() throws IOException {
    GoogleCredentials credentials = null;
    //validate service account
    if (connection.isServiceAccountJson() || connection.getServiceAccountFilePath() != null) {
      credentials =
        GCPUtils.loadServiceAccountCredentials(getServiceAccount(), isServiceAccountFilePath())
          .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
    } else {
      credentials = ServiceAccountCredentials.getApplicationDefault().createScoped(
        Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
    }
    return credentials;
  }

  public void configureDataplexException(String dataplexConfigProperty, String dataplexConfigPropType,
                                         DataplexException e,
                                         FailureCollector failureCollector) {
    if ((ERROR_CODE_NOT_FOUND == e.getCode()) || ERROR_CODE_BAD_REQUEST == e.getCode()) {
      failureCollector
        .addFailure("'" + dataplexConfigProperty + "' could not be found. Please ensure that it exists in " +
          "Dataplex.", null).withConfigProperty(dataplexConfigPropType);
    } else if (ERROR_CODE_FORBIDDEN == e.getCode()) {
      failureCollector
        .addFailure("'" + dataplexConfigProperty + "' could not be accessed. Please ensure that you have" +
          " required permissions.", null).withConfigProperty(dataplexConfigPropType);
    } else {
      failureCollector.addFailure(e.getCode() + ": " + e.getMessage(), null)
        .withConfigProperty(dataplexConfigPropType);
    }
  }

  @Nullable
  public String getServiceAccountFilePath() {
    return connection == null ? null : connection.getServiceAccountFilePath();
  }

  // get service account email for task creation
  public String getServiceAccountEmail() throws IOException {
    String email = null;
    //validate service account
    if (connection.isServiceAccountJson() || connection.getServiceAccountFilePath() != null) {
      ServiceAccountCredentials credentials =
        GCPUtils.loadServiceAccountCredentials(getServiceAccount(), isServiceAccountFilePath());
      email = credentials.getClientEmail();
    } else {
      ComputeEngineCredentials credentials = (ComputeEngineCredentials) ServiceAccountCredentials.
        getApplicationDefault();
      email = credentials.getAccount();
      // Added for preview run
      if (email.startsWith("service-")) {
        email = email.substring(8, email.indexOf("@")) + "-compute@developer.gserviceaccount.com";
      }
    }
    return email;
  }
}
