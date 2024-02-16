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

import com.google.api.client.util.Strings;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.gcp.bigquery.common.BigQueryBaseConfig;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnectorConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.CmekUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Base class for Big Query batch sink configs.
 */
public abstract class AbstractBigQuerySinkConfig extends BigQueryBaseConfig {
  public static final Set<Schema.Type> SUPPORTED_TYPES =
    ImmutableSet.of(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING, Schema.Type.FLOAT, Schema.Type.DOUBLE,
                    Schema.Type.BOOLEAN, Schema.Type.BYTES, Schema.Type.ARRAY, Schema.Type.RECORD);

  public static final String NAME_TRUNCATE_TABLE = "truncateTable";
  public static final String NAME_LOCATION = "location";
  private static final String NAME_GCS_CHUNK_SIZE = "gcsChunkSize";
  public static final String NAME_BQ_JOB_LABELS = "jobLabels";
  protected static final String NAME_UPDATE_SCHEMA = "allowSchemaRelaxation";
  private static final String SCHEME = "gs://";
  protected static final String NAME_JSON_STRING_FIELDS = "jsonStringFields";

  @Name(Constants.Reference.REFERENCE_NAME)
  @Nullable
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  protected String referenceName;

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

  @Name(NAME_BQ_JOB_LABELS)
  @Macro
  @Nullable
  @Description("Key value pairs to be added as labels to the BigQuery job. Keys must be unique. [job_source, type] " +
    "are reserved keys and cannot be used as label keys.")
  protected String jobLabelKeyValue;

  @Name(NAME_JSON_STRING_FIELDS)
  @Nullable
  @Description("Fields in input schema that should be treated as JSON strings. " +
          "The schema of these fields should be of type STRING.")
  protected String jsonStringFields;

  public AbstractBigQuerySinkConfig(BigQueryConnectorConfig connection, String dataset, String cmekKey, String bucket) {
    super(connection, dataset, cmekKey, bucket);
  }

  /**
   * Return reference name if provided, otherwise, normalize the FQN and return it as reference name
   * @return referenceName (if provided)/normalized FQN
   */
  @Nullable
  public String getReferenceName() {
    return Strings.isNullOrEmpty(referenceName)
      ? ReferenceNames.normalizeFqn(BigQueryUtil.getFQN(getDatasetProject(), dataset, getTable()))
      : referenceName;
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
  @Nullable
  public String getJobLabelKeyValue() {
    return jobLabelKeyValue;
  }

  @Nullable
  public String getJsonStringFields() {
    return jsonStringFields;
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
    if (!Strings.isNullOrEmpty(referenceName)) {
      IdUtils.validateReferenceName(referenceName, collector);
    }
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
    if (!containsMacro(NAME_BQ_JOB_LABELS)) {
      validateJobLabelKeyValue(collector);
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

  void validateJobLabelKeyValue(FailureCollector failureCollector) {
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, failureCollector, NAME_BQ_JOB_LABELS);
  }

  public String getDatasetProject() {
    return connection == null ? null : connection.getDatasetProject();
  }

}
