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

import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;

import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Base class for Big Query batch sink configs.
 */
public abstract class AbstractBigQuerySinkConfig extends GCPReferenceSinkConfig {
  private static final String SCHEME = "gs://";

  public static final String NAME_PARTITION_BY_FIELD = "partitionByField";
  public static final String NAME_CLUSTERING_ORDER = "clusteringOrder";
  public static final String NAME_OPERATION = "operation";
  public static final String NAME_TRUNCATE_TABLE = "truncateTable";
  public static final String NAME_TABLE_KEY = "relationTableKey";

  @Macro
  @Description("The dataset to write to. A dataset is contained within a specific project. "
    + "Datasets are top-level containers that are used to organize and control access to tables and views.")
  protected String dataset;

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

  @Macro
  @Nullable
  @Description("Whether to create the BigQuery table with time partitioning. This value is ignored if the table " +
    "already exists.")
  protected Boolean createPartitionedTable;

  @Name(NAME_PARTITION_BY_FIELD)
  @Macro
  @Nullable
  @Description("Partitioning column for the BigQuery table. This should be left empty if the BigQuery table is an " +
    "ingestion-time partitioned table.")
  protected String partitionByField;

  @Name(NAME_OPERATION)
  @Macro
  @Description("Type of write operation to perform. This can be set to Insert, Update or Upsert.")
  protected String operation;

  @Name(NAME_TRUNCATE_TABLE)
  @Macro
  @Description("Whether or not to truncate the table before writing to it. "
    + "Should only be used with the Insert operation.")
  protected boolean truncateTable;

  @Name(NAME_TABLE_KEY)
  @Macro
  @Nullable
  @Description("List of fields that determines relation between tables during Update and Upsert operations.")
  protected String relationTableKey;

  @Macro
  @Nullable
  @Description("Whether to create a table that requires a partition filter. This value is ignored if the table " +
    "already exists.")
  protected Boolean partitionFilterRequired;

  @Name(NAME_CLUSTERING_ORDER)
  @Macro
  @Nullable
  @Description("List of fields that determines the sort order of the data. Fields must be of type INT, LONG, " +
    "STRING, DATE, TIMESTAMP, BOOLEAN or DECIMAL. Tables cannot be clustered on more than 4 fields. This value is " +
    "only used when the BigQuery table is automatically created and ignored if the table already exists.")
  protected String clusteringOrder;

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

  public boolean shouldCreatePartitionedTable() {
    return createPartitionedTable == null ? false : createPartitionedTable;
  }

  @Nullable
  public String getPartitionByField() {
    return partitionByField;
  }

  public boolean isPartitionFilterRequired() {
    return partitionFilterRequired == null ? false : partitionFilterRequired;
  }

  @Nullable
  public String getClusteringOrder() {
    return clusteringOrder;
  }

  public Operation getOperation() {
    return Operation.valueOf(operation.toUpperCase());
  }

  public WriteDisposition getWriteDisposition() {
    return truncateTable ? WriteDisposition.WRITE_TRUNCATE : WriteDisposition.WRITE_APPEND;
  }

  @Nullable
  public String getRelationTableKey() {
    return relationTableKey;
  }

  @Override
  public void validate() {
    super.validate();
    String bucket = getBucket();
    if (!containsMacro("bucket") && bucket != null) {
      // Basic validation for allowed characters as per https://cloud.google.com/storage/docs/naming
      Pattern p = Pattern.compile("[a-z0-9._-]+");
      if (!p.matcher(bucket).matches()) {
        throw new InvalidConfigPropertyException("Bucket names can only contain lowercase characters, numbers, " +
                                                   "'.', '_', and '-'.", "bucket");
      }
    }

    if (getWriteDisposition().equals(WriteDisposition.WRITE_TRUNCATE) && !getOperation().equals(Operation.INSERT)) {
      throw new InvalidConfigPropertyException("Truncate may only be used with operation Insert", NAME_TRUNCATE_TABLE);
    }
  }
}
