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

import com.google.auth.Credentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;

import javax.annotation.Nullable;

/**
 * Common configuration class for BigQuery sources, sinks and engines.
 */
public class BigQueryBaseConfig extends GCPConfig {
  private static final String SCHEME = "gs://";

  public static final String DATASET_PROJECT_ID = "datasetProject";
  public static final String NAME_DATASET = "dataset";
  public static final String NAME_BUCKET = "bucket";

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
    + "Cloud Storage data will be deleted after it is loaded into BigQuery. "
    + "If it is not provided, a unique bucket will be automatically created and then deleted after the run finishes. "
    + "The service account must have permission to create buckets in the configured project.")
  protected String bucket;

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

  /* returns the bucket if it exists otherwise null.
   */
  private Bucket getBucketIfExists(Storage storage, String bucketName) {
    Bucket bucket = null;
    try {
      bucket = storage.get(bucketName);
    } catch (StorageException e) {
      // If there is an exception getting bucket information during config validation, it will be ignored because
      // service account used can be different.
    }
    return bucket;
  }

  public void validateCmekKeyLocation(@Nullable CryptoKeyName cmekKeyName, @Nullable String tableName,
                                      @Nullable String location, FailureCollector failureCollector) {
    if (cmekKeyName == null || containsMacro(NAME_DATASET) || projectOrServiceAccountContainsMacro()
      || containsMacro(NAME_BUCKET)) {
      return;
    }
    String datasetProjectId = getDatasetProject();
    String datasetName = getDataset();
    DatasetId datasetId = DatasetId.of(datasetProjectId, datasetName);
    TableId tableId = tableName == null ? null : TableId.of(datasetProjectId, datasetName, tableName);
    Credentials credentials = getCredentials(failureCollector);
    BigQuery bigQuery = GCPUtils.getBigQuery(getProject(), credentials);
    Storage storage = GCPUtils.getStorage(getProject(), credentials);
    if (bigQuery == null || storage == null) {
      return;
    }
    String bucketName = getBucket();
    Bucket bucket = bucketName == null ? null : getBucketIfExists(storage, bucketName);
    // if bucket exists then dataset and table will be created in location of bucket if they do not exist.
    location = bucket == null ? location : bucket.getLocation();
    Dataset dataset = CmekUtils.validateCmekKeyAndDatasetOrTableLocation(bigQuery, datasetId, tableId, cmekKeyName,
                                                                         location, failureCollector);
    if (bucket == null && dataset != null) {
      // if dataset exists then bucket will be created in the location of dataset.
      location = dataset.getLocation();
      GCSPath gcsPath = Strings.isNullOrEmpty(bucketName) ? null : GCSPath.from(bucketName);
      CmekUtils.validateCmekKeyAndBucketLocation(storage, gcsPath, cmekKeyName, location, failureCollector);
    }
  }
}
