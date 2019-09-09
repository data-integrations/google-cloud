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
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;

import java.util.Set;
import java.util.regex.Pattern;
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

  @Macro
  @Description("Whether to modify the BigQuery table schema if it differs from the input schema.")
  protected boolean allowSchemaRelaxation;

  @Name(NAME_TRUNCATE_TABLE)
  @Macro
  @Nullable
  @Description("Whether or not to truncate the table before writing to it. "
    + "Should only be used with the Insert operation.")
  protected Boolean truncateTable;

  @Nullable
  protected String getTable() {
    return null;
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

  public boolean isAllowSchemaRelaxation() {
    return allowSchemaRelaxation;
  }

  public JobInfo.WriteDisposition getWriteDisposition() {
    return truncateTable != null && truncateTable ? JobInfo.WriteDisposition.WRITE_TRUNCATE
      : JobInfo.WriteDisposition.WRITE_APPEND;
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);
    String bucket = getBucket();
    if (!containsMacro(NAME_BUCKET) && bucket != null) {
      // Basic validation for allowed characters as per https://cloud.google.com/storage/docs/naming
      Pattern p = Pattern.compile("[a-z0-9._-]+");
      if (!p.matcher(bucket).matches()) {
        collector.addFailure("Bucket must only contain lowercase characters, numbers, '.', '_', and '-'", null)
          .withConfigProperty(NAME_BUCKET);
      }
    }
  }
}
