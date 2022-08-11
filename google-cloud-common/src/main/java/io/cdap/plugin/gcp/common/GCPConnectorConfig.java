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

package io.cdap.plugin.gcp.common;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Arguments;
import io.cdap.cdap.etl.api.FailureCollector;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Plugin configuration for GCP connectors.
 */
public class GCPConnectorConfig extends PluginConfig {
  public static final String NAME_PROJECT = "project";
  public static final String NAME_SERVICE_ACCOUNT_TYPE = "serviceAccountType";
  public static final String NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";
  public static final String NAME_SERVICE_ACCOUNT_JSON = "serviceAccountJSON";
  public static final String AUTO_DETECT = "auto-detect";
  public static final String SERVICE_ACCOUNT_FILE_PATH = "filePath";
  public static final String SERVICE_ACCOUNT_JSON = "JSON";

  @Name(NAME_PROJECT)
  @Description("Google Cloud Project ID. It can be found on the Dashboard in the Google Cloud Platform Console.")
  @Macro
  @Nullable
  protected String project;

  @Name(NAME_SERVICE_ACCOUNT_TYPE)
  @Description("Service account type, file path where the service account is located or the JSON content of the " +
    "service account.")
  @Macro
  @Nullable
  protected String serviceAccountType;

  @Name(NAME_SERVICE_ACCOUNT_FILE_PATH)
  @Description("Path on the local file system of the service account key used " +
    "for authorization. Can be set to 'auto-detect' when running on a Dataproc cluster. " +
    "When running on other clusters, the file must be present on every node in the cluster.")
  @Macro
  @Nullable
  protected String serviceFilePath;

  @Name(NAME_SERVICE_ACCOUNT_JSON)
  @Description("Content of the service account file.")
  @Macro
  @Nullable
  protected String serviceAccountJson;

  public GCPConnectorConfig(@Nullable String project, @Nullable String serviceAccountType,
                            @Nullable String serviceFilePath, @Nullable String serviceAccountJson) {
    this.project = project;
    this.serviceAccountType = serviceAccountType;
    this.serviceFilePath = serviceFilePath;
    this.serviceAccountJson = serviceAccountJson;
  }

  public String getProject() {
    String projectId = tryGetProject();
    if (projectId == null) {
      throw new IllegalArgumentException(
        "Could not detect Google Cloud project id from the environment. Please specify a project id.");
    }
    return projectId;
  }

  @Nullable
  public String tryGetProject() {
    if (containsMacro(NAME_PROJECT) && Strings.isNullOrEmpty(project)) {
      return null;
    }
    String projectId = project;
    if (Strings.isNullOrEmpty(project) || AUTO_DETECT.equals(project)) {
      projectId = ServiceOptions.getDefaultProjectId();
    }
    return projectId;
  }

  @Nullable
  public String getServiceAccountFilePath() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) || serviceFilePath == null || serviceFilePath.isEmpty() ||
      AUTO_DETECT.equals(serviceFilePath)) {
      return null;
    }
    return serviceFilePath;
  }

  @Nullable
  public String getServiceAccountJson() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_JSON) || Strings.isNullOrEmpty(serviceAccountJson)) {
      return null;
    }
    return serviceAccountJson;
  }

  /**
   * @return Service Account Type, defaults to filePath.
   */
  @Nullable
  public String getServiceAccountType() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_TYPE)) {
      return null;
    }
    return Strings.isNullOrEmpty(serviceAccountType) ? SERVICE_ACCOUNT_FILE_PATH : serviceAccountType;
  }

  @Nullable
  public Boolean isServiceAccountJson() {
    String serviceAccountType = getServiceAccountType();
    return Strings.isNullOrEmpty(serviceAccountType) ? null : serviceAccountType.equals(SERVICE_ACCOUNT_JSON);
  }

  @Nullable
  public Boolean isServiceAccountFilePath() {
    String serviceAccountType = getServiceAccountType();
    return Strings.isNullOrEmpty(serviceAccountType) ? null : serviceAccountType.equals(SERVICE_ACCOUNT_FILE_PATH);
  }

  @Nullable
  public String getServiceAccount() {
    Boolean serviceAccountJson = isServiceAccountJson();
    if (serviceAccountJson == null) {
      return null;
    }
    return serviceAccountJson ? getServiceAccountJson() : getServiceAccountFilePath();
  }

  /**
   * Returns true if connector connection properties don't contain any macros.
   */
  public boolean canConnect() {
    return !containsMacro(GCPConnectorConfig.NAME_SERVICE_ACCOUNT_TYPE) &&
      !(containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) || containsMacro(NAME_SERVICE_ACCOUNT_JSON)) &&
      !containsMacro(NAME_PROJECT);
  }
  
  @Nullable
  public Credentials getCredentials(FailureCollector collector) {
    Boolean isServiceAccountFilePath = isServiceAccountFilePath();
    Credentials credentials = null;
    try {
      credentials = getServiceAccount() == null ?
        null : GCPUtils.loadServiceAccountCredentials(getServiceAccount(), isServiceAccountFilePath);
    } catch (IOException e) {
      collector.addFailure(e.getMessage(), null);
    }
    return credentials;
  }

  /**
   * Return true if the service account is set to auto-detect but it can't be fetched from the environment.
   * This shouldn't result in a deployment failure, as the credential could be detected at runtime if the pipeline
   * runs on dataproc. This should primarily be used to check whether certain validation logic should be skipped.
   *
   * @return true if the service account is set to auto-detect but it can't be fetched from the environment.
   */
  public boolean autoServiceAccountUnavailable() {
    if (getServiceAccountFilePath() == null && SERVICE_ACCOUNT_FILE_PATH.equals(getServiceAccountType())) {
      try {
        ServiceAccountCredentials.getApplicationDefault();
      } catch (IOException e) {
        return true;
      }
    }
    return false;
  }
}
