/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.source;

import com.google.cloud.ServiceOptions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;

import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Holds configuration required for configuring {@link BigQuerySource}.
 */
public final class BigQuerySourceConfig extends GCPReferenceSourceConfig {
  private static final String SCHEME = "gs://";
  private static final String WHERE = "WHERE";
  public static final Set<Schema.Type> SUPPORTED_TYPES =
    ImmutableSet.of(Schema.Type.LONG, Schema.Type.STRING, Schema.Type.DOUBLE, Schema.Type.BOOLEAN, Schema.Type.BYTES,
                    Schema.Type.ARRAY, Schema.Type.RECORD);

  public static final String NAME_DATASET = "dataset";
  public static final String NAME_TABLE = "table";
  public static final String NAME_BUCKET = "bucket";
  public static final String NAME_SCHEMA = "schema";
  public static final String NAME_DATASET_PROJECT = "datasetProject";
  public static final String NAME_PARTITION_FROM = "partitionFrom";
  public static final String NAME_PARTITION_TO = "partitionTo";
  public static final String NAME_FILTER = "filter";

  @Name(NAME_DATASET)
  @Macro
  @Description("The dataset the table belongs to. A dataset is contained within a specific project. "
    + "Datasets are top-level containers that are used to organize and control access to tables and views.")
  private String dataset;

  @Name(NAME_TABLE)
  @Macro
  @Description("The table to read from. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  private String table;

  @Name(NAME_BUCKET)
  @Macro
  @Nullable
  @Description("The Google Cloud Storage bucket to store temporary data in. "
    + "It will be automatically created if it does not exist, but will not be automatically deleted. "
    + "Temporary data will be deleted after it has been read. "
    + "If it is not provided, a unique bucket will be created and then deleted after the run finishes. "
    + "The service account must have permission to create buckets in the configured project.")
  private String bucket;

  @Name(NAME_SCHEMA)
  @Macro
  @Nullable
  @Description("The schema of the table to read.")
  private String schema;

  @Name(NAME_DATASET_PROJECT)
  @Macro
  @Nullable
  @Description("The project the dataset belongs to. This is only required if the dataset is not "
    + "in the same project that the BigQuery job will run in. If no value is given, it will default to the configured "
    + "project ID.")
  private String datasetProject;

  @Name(NAME_PARTITION_FROM)
  @Macro
  @Nullable
  @Description("It's inclusive partition start date. It should be a String with format \"yyyy-MM-dd\". " +
    "This value is ignored if the table does not support partitioning.")
  private String partitionFrom;

  @Name(NAME_PARTITION_TO)
  @Macro
  @Nullable
  @Description("It's inclusive partition end date. It should be a String with format \"yyyy-MM-dd\". " +
    "This value is ignored if the table does not support partitioning.")
  private String partitionTo;

  @Name(NAME_FILTER)
  @Macro
  @Nullable
  @Description("The WHERE clause filters out rows by evaluating each row against boolean expression, " +
          "and discards all rows that do not return TRUE (that is, rows that return FALSE or NULL).")
  private String filter;

  public String getDataset() {
    return dataset;
  }

  public String getTable() {
    return table;
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

  public String getDatasetProject() {
    if (GCPConfig.AUTO_DETECT.equalsIgnoreCase(datasetProject)) {
      return ServiceOptions.getDefaultProjectId();
    }
    return Strings.isNullOrEmpty(datasetProject) ? getProject() : datasetProject;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);
    String bucket = getBucket();

    if (!containsMacro(NAME_BUCKET)) {
      BigQueryUtil.validateBucket(bucket, NAME_BUCKET, collector);
    }

    if (!containsMacro(NAME_DATASET)) {
      BigQueryUtil.validateDataset(dataset, NAME_DATASET, collector);
    }

    if (!containsMacro(NAME_TABLE)) {
      BigQueryUtil.validateTable(table, NAME_TABLE, collector);
    }
  }

  /**
   * @return the schema of the dataset
   */
  @Nullable
  public Schema getSchema(FailureCollector collector) {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null).withConfigProperty(NAME_SCHEMA);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  @Nullable
  public String getPartitionFrom() {
    return Strings.isNullOrEmpty(partitionFrom) ? null : partitionFrom;
  }

  @Nullable
  public String getPartitionTo() {
    return Strings.isNullOrEmpty(partitionTo) ? null : partitionTo;
  }

  @Nullable
  public String getFilter() {
    if (filter != null) {
      filter = filter.trim();
      if (filter.isEmpty()) {
        return null;
      }
      // remove the WHERE keyword from the filter if the user adds it at the begging of the expression
      if (filter.toUpperCase().startsWith(WHERE)) {
        filter = filter.substring(WHERE.length());
      }
    }
    return filter;
  }

  /**
   * Returns true if bigquery table can be connected and schema is not a macro.
   */
  public boolean canConnect() {
    return !containsMacro(NAME_SCHEMA) && !containsMacro(NAME_DATASET) && !containsMacro(NAME_TABLE) &&
      !containsMacro(NAME_DATASET_PROJECT) && !containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) &&
      !containsMacro(NAME_PROJECT);
  }
}
