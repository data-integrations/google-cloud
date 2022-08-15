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

package io.cdap.plugin.gcp.bigquery.connector;

import com.google.cloud.ServiceOptions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;

import javax.annotation.Nullable;

/**
 * Plugin config for BiqueryConnector
 */
public class BigQueryConnectorConfig extends GCPConnectorConfig {
  public static final String NAME_DATASET_PROJECT = "datasetProject";

  @Name(NAME_DATASET_PROJECT)
  @Macro
  @Nullable
  @Description("The project the dataset belongs to. This is only required if the dataset is not " +
    "in the same project that the BigQuery job will run in. If no value is given, it will" +
    " default to the configured project ID.")
  private String datasetProject;

  public String getDatasetProject() {
    // if it's "auto-detect" that means we need to detect the default project settings
    // for sandbox you can use `gcloud config set project my-project-id" to set it
    // or you start the sandbox by specify the java argument `-DGOOGLE_CLOUD_PROJECT=my-project-id`
    // or you start the sandbox by specify the java argument `-DGCLOUD_PROJECT=my-project-id`
    // otherwise we will throw IllegalArgument exception here same as project
    if (AUTO_DETECT.equalsIgnoreCase(datasetProject)) {
      String defaultProject = ServiceOptions.getDefaultProjectId();
      if (defaultProject == null) {
        throw new IllegalArgumentException(
          "Could not detect Google Cloud project id from the environment. Please specify a dataset project id.");
      }
      return defaultProject;
    }
    // if it's null or empty that means it should be same as project
    return Strings.isNullOrEmpty(datasetProject) ? getProject() : datasetProject;
  }

  /**
   * Returns true if bigquery connection properties don't contain any Macro
   */
  public boolean canConnect() {
    return super.canConnect() && !containsMacro(NAME_DATASET_PROJECT);
  }

  public BigQueryConnectorConfig(@Nullable String project, @Nullable String datasetProject,
    @Nullable String serviceAccountType, @Nullable String serviceFilePath, @Nullable String serviceAccountJson) {
    super(project, serviceAccountType, serviceFilePath, serviceAccountJson);
    this.datasetProject = datasetProject;
  }
}
