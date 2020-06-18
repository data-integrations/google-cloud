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

import com.google.cloud.bigquery.JobInfo;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * Base class for Big Query batch sink configs.
 */
public abstract class AbstractBigQuerySinkConfig extends GCPReferenceSinkConfig {
  private static final String SCHEME = "gs://";
  public static final Set<Schema.Type> SUPPORTED_TYPES =
    ImmutableSet.of(Schema.Type.INT, Schema.Type.LONG, Schema.Type.STRING, Schema.Type.FLOAT, Schema.Type.DOUBLE,
                    Schema.Type.BOOLEAN, Schema.Type.BYTES, Schema.Type.ARRAY, Schema.Type.RECORD);

  public static final String NAME_DATASET = "dataset";
  public static final String NAME_BUCKET = "bucket";
  public static final String NAME_TRUNCATE_TABLE = "truncateTable";
  public static final String NAME_LOCATION = "location";
  private static final String NAME_GCS_CHUNK_SIZE = "gcsChunkSize";
  private static final String NAME_UPDATE_SCHEMA = "allowSchemaRelaxation";

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
    + "If it is not provided, a unique bucket will be created and then deleted after the run finishes.")
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

  @Nullable
  public String getLocation() {
    return location;
  }

  @Nullable
  protected String getTable() {
    return null;
  }

  public String getDataset() {
    return dataset;
  }

  @Nullable
  public String getGcsChunkSize() {
    return gcsChunkSize;
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

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);
    String bucket = getBucket();
    if (!containsMacro(NAME_BUCKET)) {
      BigQueryUtil.validateBucket(bucket, NAME_BUCKET, collector);
    }

    if (!containsMacro(NAME_DATASET)) {
      BigQueryUtil.validateDataset(dataset, NAME_DATASET, collector);
    }

  }
}
