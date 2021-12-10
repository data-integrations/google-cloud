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

package io.cdap.plugin.gcp.bigquery.sqlengine;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnectorConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Configuration for SQL Engine.
 */
public class BigQuerySQLEngineConfig extends PluginConfig {

  public static final String NAME_LOCATION = "location";
  public static final String NAME_RETAIN_TABLES = "retainTables";
  public static final String NAME_TEMP_TABLE_TTL_HOURS = "tempTableTTLHours";
  public static final String NAME_JOB_PRIORITY = "jobPriority";
  public static final String NAME_USE_STORAGE_READ_API = "useStorageReadAPI";
  public static final String NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";
  public static final String NAME_SERVICE_ACCOUNT_JSON = "serviceAccountJSON";
  public static final String SERVICE_ACCOUNT_JSON = "JSON";
  public static final String AUTO_DETECT = "auto-detect";

  // Job priority options
  public static final String PRIORITY_BATCH = "batch";
  public static final String PRIORITY_INTERACTIVE = "interactive";
  private static final String NAME_BUCKET = "bucket";
  private static final String NAME_CMEK_KEY = "cmekKey";
  protected static final String NAME_DATASET = "dataset";
  private static final String SCHEME = "gs://";

  @Name(NAME_DATASET)
  @Macro
  @Description("The dataset to write to. A dataset is contained within a specific project. "
          + "Datasets are top-level containers that are used to organize and control access to tables and views.")
  protected String dataset;

  @Name(NAME_LOCATION)
  @Macro
  @Nullable
  @Description("The location where the BigQuery dataset will get created. " +
    "This value is ignored if the dataset or temporary bucket already exists.")
  protected String location;

  @Name(NAME_BUCKET)
  @Macro
  @Nullable
  @Description("The Google Cloud Storage bucket to store temporary data in. "
          + "Cloud Storage data will be deleted after it is loaded into BigQuery. "
          + "If it is not provided, a unique bucket will be automatically created and then deleted after the run " +
          "finishes. The service account must have permission to create buckets in the configured project.")
  protected String bucket;

  @Name(NAME_CMEK_KEY)
  @Macro
  @Nullable
  @Description("The GCP customer managed encryption key (CMEK) name used to encrypt data written to " +
    "any bucket, dataset, or table created by the plugin. If the bucket, dataset, or table already exists, " +
    "this is ignored.")
  protected String cmekKey;

  @Name(NAME_RETAIN_TABLES)
  @Macro
  @Nullable
  @Description("Select this option to retain all BigQuery temporary tables created during the pipeline run.")
  protected Boolean retainTables;

  @Name(NAME_TEMP_TABLE_TTL_HOURS)
  @Macro
  @Nullable
  @Description("Set table TTL for temporary BigQuery tables, in number of hours. Tables will be deleted " +
    "automatically on pipeline completion.")
  protected Integer tempTableTTLHours;

  @Name(NAME_JOB_PRIORITY)
  @Macro
  @Nullable
  @Description("Priority used to execute BigQuery Jobs. The value must be 'batch' or 'interactive'. " +
    "An interactive job is executed as soon as possible and counts towards the concurrent rate " +
    "limit and the daily rate limit. A batch job is queued and started as soon as idle resources " +
    "are available, usually within a few minutes. If the job hasn't started within 3 hours, " +
    "its priority is changed to 'interactive'")
  private String jobPriority;
  
  @Name(ConfigUtil.NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  private Boolean useConnection;

  @Name(ConfigUtil.NAME_CONNECTION)
  @Macro
  @Nullable
  @Description("The existing connection to use.")
  protected BigQueryConnectorConfig connection;
  
  @Name(NAME_USE_STORAGE_READ_API)
  @Macro
  @Nullable
  @Description("Select this option to use the BigQuery Storage Read API when extracting records from BigQuery " +
    "during pipeline execution. This option can increase the performance of the BigQuery ELT Transformation " +
    "Pushdown execution. The usage of this API incurrs in additional costs. This requires Scala version 2.12 to be " +
    "installed in the execution environment.")
  private Boolean useStorageReadAPI;

  @Name(NAME_SERVICE_ACCOUNT_FILE_PATH)
  @Description("Path on the local file system of the service account key used "
          + "for authorization. Can be set to 'auto-detect' when running on a Dataproc cluster. "
          + "When running on other clusters, the file must be present on every node in the cluster.")
  @Macro
  @Nullable
  protected String serviceFilePath;

  @Name(NAME_SERVICE_ACCOUNT_JSON)
  @Description("Content of the service account file.")
  @Macro
  @Nullable
  protected String serviceAccountJson;


  private BigQuerySQLEngineConfig(@Nullable String project, @Nullable String serviceAccountType,
                                  @Nullable String serviceFilePath, @Nullable String serviceAccountJson,
                                  @Nullable String dataset, @Nullable String location,
                                  @Nullable String cmekKey, @Nullable String bucket) {
    this.dataset = dataset;
    this.location = location;
    this.cmekKey = cmekKey;
    this.bucket = bucket;
    this.connection = new BigQueryConnectorConfig(project, project, serviceAccountType,
            serviceFilePath, serviceAccountJson);
  }

  private BigQuerySQLEngineConfig(@Nullable String project, @Nullable String datasetProject,
                                  @Nullable String serviceAccountType,
                                  @Nullable String serviceFilePath, @Nullable String serviceAccountJson,
                                  @Nullable String dataset, @Nullable String location,
                                  @Nullable String cmekKey, @Nullable String bucket) {
    this.dataset = dataset;
    this.location = location;
    this.cmekKey = cmekKey;
    this.bucket = bucket;
    this.connection = new BigQueryConnectorConfig(project, datasetProject, serviceAccountType,
            serviceFilePath, serviceAccountJson);
  }

  public Boolean shouldRetainTables() {
    return retainTables != null ? retainTables : false;
  }

  public Integer getTempTableTTLHours() {
    return tempTableTTLHours != null && tempTableTTLHours > 0 ? tempTableTTLHours : 72;
  }

  public Boolean shouldUseStorageReadAPI() {
    return useStorageReadAPI != null ? useStorageReadAPI : false;
  }

  public QueryJobConfiguration.Priority getJobPriority() {
    String priority = jobPriority != null ? jobPriority : "batch";
    return QueryJobConfiguration.Priority.valueOf(priority.toUpperCase());
  }

  /**
   * Validates configuration properties
   */
  public void validate() {
    // Ensure value for the job priority configuration property is valid
    if (jobPriority != null && !containsMacro(NAME_JOB_PRIORITY)
      && !PRIORITY_BATCH.equalsIgnoreCase(jobPriority)
      && !PRIORITY_INTERACTIVE.equalsIgnoreCase(jobPriority)) {
      throw new SQLEngineException("Property 'jobPriority' must be 'batch' or 'interactive'");
    }
  }

  public void validate(FailureCollector failureCollector) {
    validate(failureCollector, Collections.emptyMap());
  }

  public void validate(FailureCollector failureCollector, Map<String, String> arguments) {
    validate();
    ConfigUtil.validateConnection(this, useConnection, connection, failureCollector);
    String bucket = getBucket();
    if (!containsMacro(NAME_BUCKET)) {
      BigQueryUtil.validateBucket(bucket, NAME_BUCKET, failureCollector);
    }
    if (!containsMacro(NAME_DATASET)) {
      BigQueryUtil.validateDataset(dataset, NAME_DATASET, failureCollector);
    }
    if (!containsMacro(NAME_CMEK_KEY)) {
      validateCmekKey(failureCollector, arguments);
    }
  }

  void validateCmekKey(FailureCollector failureCollector, Map<String, String> arguments) {
    CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(cmekKey, arguments, failureCollector);
    if (containsMacro(NAME_LOCATION)) {
      return;
    }
    validateCmekKeyLocation(cmekKeyName, null, location, failureCollector);
  }


  public String getDataset() {
    return dataset;
  }

  public String getDatasetProject() {
    return connection == null ? null : connection.getDatasetProject();
  }

  public String getProject() {
    if (connection == null) {
      throw new IllegalArgumentException(
              "Could not get project information, connection should not be null!");
    }
    return connection.getProject();
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
  public Boolean isServiceAccountFilePath() {
    return connection == null ? null : connection.isServiceAccountFilePath();
  }

  @Nullable
  public String getServiceAccountType() {
    return connection == null ? null : connection.getServiceAccountType();
  }

  @Nullable
  public String getServiceAccountFilePath() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) || serviceFilePath == null ||
            serviceFilePath.isEmpty() || AUTO_DETECT.equals(serviceFilePath)) {
      return null;
    }
    return serviceFilePath;
  }

  @Nullable
  public Boolean isServiceAccountJson() {
    String serviceAccountType = getServiceAccountType();
    return Strings.isNullOrEmpty(serviceAccountType) ? null : serviceAccountType.equals(SERVICE_ACCOUNT_JSON);
  }

  @Nullable
  public String getServiceAccountJson() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_JSON) || Strings.isNullOrEmpty(serviceAccountJson)) {
      return null;
    }
    return serviceAccountJson;
  }

  @Nullable
  public String getLocation() {
    return location;
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
    if (cmekKeyName == null || containsMacro(NAME_DATASET) || connection == null || !connection.canConnect()
            || containsMacro(NAME_BUCKET)) {
      return;
    }
    String datasetProjectId = connection.getDatasetProject();
    String datasetName = getDataset();
    DatasetId datasetId = DatasetId.of(datasetProjectId, datasetName);
    TableId tableId = tableName == null ? null : TableId.of(datasetProjectId, datasetName, tableName);
    Credentials credentials = connection.getCredentials(failureCollector);
    BigQuery bigQuery = GCPUtils.getBigQuery(connection.getProject(), credentials);
    Storage storage = GCPUtils.getStorage(connection.getProject(), credentials);
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

  public static Builder builder() {
    return new Builder();
  }

  /**
   * BigQuery SQlEngine configuration builder.
   */
  public static class Builder {
    private String serviceAccountType;
    private String serviceFilePath;
    private String serviceAccountJson;
    private String project;
    private String dataset;
    private String cmekKey;
    private String location;
    private String bucket;
    

    public Builder setProject(@Nullable String project) {
      this.project = project;
      return this;
    }

    public Builder setServiceAccountType(@Nullable String serviceAccountType) {
      this.serviceAccountType = serviceAccountType;
      return this;
    }

    public Builder setServiceFilePath(@Nullable String serviceFilePath) {
      this.serviceFilePath = serviceFilePath;
      return this;
    }

    public Builder setServiceAccountJson(@Nullable String serviceAccountJson) {
      this.serviceAccountJson = serviceAccountJson;
      return this;
    }

    public Builder setDataset(@Nullable String dataset) {
      this.dataset = dataset;
      return this;
    }

    public Builder setCmekKey(@Nullable String cmekKey) {
      this.cmekKey = cmekKey;
      return this;
    }

    public Builder setLocation(@Nullable String location) {
      this.location = location;
      return this;
    }

    public Builder setBucket(@Nullable String bucket) {
      this.bucket = bucket;
      return this;
    }

    public BigQuerySQLEngineConfig build() {
      return new BigQuerySQLEngineConfig(
        project,
        serviceAccountType,
        serviceFilePath,
        serviceAccountJson,
        dataset,
        location,
        cmekKey,
        bucket
      );
    }
  }
}
