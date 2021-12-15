/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.plugin.gcp.bigquery.sink;

import com.google.auth.Credentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.gcp.bigquery.common.BigQueryBaseConfig;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnectorConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Base class for Big Query batch sink configs.
 */
public abstract class AbstractBigQuerySinkConfig extends PluginConfig {
  public static final Set<Schema.Type> SUPPORTED_TYPES =
    ImmutableSet.of(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING, Schema.Type.FLOAT, Schema.Type.DOUBLE,
                    Schema.Type.BOOLEAN, Schema.Type.BYTES, Schema.Type.ARRAY, Schema.Type.RECORD);

  public static final String NAME_TRUNCATE_TABLE = "truncateTable";
  public static final String NAME_LOCATION = "location";
  private static final String NAME_GCS_CHUNK_SIZE = "gcsChunkSize";
  protected static final String NAME_UPDATE_SCHEMA = "allowSchemaRelaxation";
  private static final String SCHEME = "gs://";
  protected static final String NAME_DATASET = "dataset";
  private static final String NAME_BUCKET = "bucket";
  private static final String NAME_CMEK_KEY = "cmekKey";

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  protected String referenceName;

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

  @Name(NAME_GCS_CHUNK_SIZE)
  @Macro
  @Nullable
  @Description("Optional property to tune chunk size in gcs upload request. The value of this property should be in " +
    "number of bytes. By default, 8388608 bytes (8MB) will be used as upload request chunk size.")
  protected String gcsChunkSize;

  @Name(NAME_UPDATE_SCHEMA)
  @Macro
  @Nullable
  @Description("Whether to modify the BigQuery table schema if it differs from the input schema.")
  protected Boolean allowSchemaRelaxation;

  @Name(NAME_TRUNCATE_TABLE)
  @Macro
  @Nullable
  @Description("Whether or not to truncate the table before writing to it. "
    + "Should only be used with the Insert operation. This could overwrite the table schema")
  protected Boolean truncateTable;

  @Name(NAME_LOCATION)
  @Macro
  @Nullable
  @Description("The location where the big query dataset will get created. " +
    "This value is ignored if the dataset or temporary bucket already exist.")
  protected String location;

  @Name(NAME_CMEK_KEY)
  @Macro
  @Nullable
  @Description("The GCP customer managed encryption key (CMEK) name used to encrypt data written to " +
    "any bucket, dataset, or table created by the plugin. If the bucket, dataset, or table already exists, " +
    "this is ignored.")
  protected String cmekKey;

  @Name(ConfigUtil.NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  private Boolean useConnection;

  @Name(ConfigUtil.NAME_CONNECTION)
  @Macro
  @Nullable
  @Description("The existing connection to use.")
  protected BigQueryConnectorConfig connection;

  public String getReferenceName() {
    return referenceName;
  }

  @Nullable
  public String getLocation() {
    return location;
  }

  @Nullable
  protected String getTable() {
    return null;
  }

  @Nullable
  public String getGcsChunkSize() {
    return gcsChunkSize;
  }

  public boolean isAllowSchemaRelaxation() {
    return allowSchemaRelaxation == null ? false : allowSchemaRelaxation;
  }

  public JobInfo.WriteDisposition getWriteDisposition() {
    return isTruncateTableSet() ? JobInfo.WriteDisposition.WRITE_TRUNCATE
      : JobInfo.WriteDisposition.WRITE_APPEND;
  }

  public boolean isTruncateTableSet() {
    return truncateTable != null && truncateTable;
  }

  public void validate(FailureCollector collector) {
    validate(collector, Collections.emptyMap());
  }
  
  public void validate(FailureCollector collector, Map<String, String> arguments) {
    IdUtils.validateReferenceName(referenceName, collector);
    ConfigUtil.validateConnection(this, useConnection, connection, collector);
    String bucket = getBucket();
    if (!containsMacro(NAME_BUCKET)) {
      BigQueryUtil.validateBucket(bucket, NAME_BUCKET, collector);
    }
    if (!containsMacro(NAME_GCS_CHUNK_SIZE)) {
      BigQueryUtil.validateGCSChunkSize(gcsChunkSize, NAME_GCS_CHUNK_SIZE, collector);
    }
    if (!containsMacro(NAME_DATASET)) {
      BigQueryUtil.validateDataset(dataset, NAME_DATASET, collector);
    }
    if (!containsMacro(NAME_CMEK_KEY)) {
      validateCmekKey(collector, arguments);
    }
  }

  void validateCmekKey(FailureCollector failureCollector, Map<String, String> arguments) {
    CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(cmekKey, arguments, failureCollector);
    //these fields are needed to check if bucket exists or not and for location validation
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
}
