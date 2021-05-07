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

package io.cdap.plugin.gcp.bigquery.common;

import com.google.cloud.ServiceOptions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.gcp.common.GCPConfig;

import javax.annotation.Nullable;

/**
 * Common configuration class for BigQuery sources, sinks and engines.
 */
public class BigQueryBaseConfig extends GCPConfig {
  private static final String SCHEME = "gs://";

  public static final String DATASET_PROJECT_ID = "datasetProject";
  public static final String NAME_DATASET = "dataset";
  public static final String NAME_BUCKET = "bucket";

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  public String referenceName;

  @Name(DATASET_PROJECT_ID)
  @Macro
  @Nullable
  @Description("The project in which the dataset is located/should be created."
    + " Defaults to the project specified in the Project Id property.")
  private String datasetProject;

  @Name(NAME_DATASET)
  @Macro
  @Description("The dataset to write to. A dataset is contained within a specific project. "
    + "Datasets are top-level containers that are used to organize and control access to tables and views.")
  protected String dataset;

  @Name(NAME_BUCKET)
  @Macro
  @Nullable
  @Description("The Google Cloud Storage bucket to store temporary data in. "
    + "It will be automatically created if it does not exist, but will not be automatically deleted. "
    + "Cloud Storage data will be deleted after it is loaded into BigQuery. "
    + "If it is not provided, a unique bucket will be created and then deleted after the run finishes. "
    + "The service account must have permission to create buckets in the configured project.")
  protected String bucket;

  /**
   * Validates the given referenceName to consists of characters allowed to represent a dataset.
   */
  public void validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);
  }

  public String getReferenceName() {
    return referenceName;
  }

  @Nullable
  public String getDatasetProject() {
    if (GCPConfig.AUTO_DETECT.equalsIgnoreCase(datasetProject)) {
      return ServiceOptions.getDefaultProjectId();
    }
    return Strings.isNullOrEmpty(datasetProject) ? getProject() : datasetProject;
  }

  public String getDataset() {
    return dataset;
  }

  @Nullable
  public String getBucket() {
    if (bucket != null) {
      bucket = bucket.trim();
      if (bucket.isEmpty()) {
        return null;
      }
      // remove the gs:// scheme from the bucket name
      if (bucket.startsWith(SCHEME)) {
        bucket = bucket.substring(SCHEME.length());
      }
    }
    return bucket;
  }

}
