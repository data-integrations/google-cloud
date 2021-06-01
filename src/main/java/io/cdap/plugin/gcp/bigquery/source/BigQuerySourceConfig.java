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

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition.Type;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnectorConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;

import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Holds configuration required for configuring {@link BigQuerySource}.
 */
public final class BigQuerySourceConfig extends PluginConfig {
  private static final String SCHEME = "gs://";
  private static final String WHERE = "WHERE";
  public static final Set<Schema.Type> SUPPORTED_TYPES =
    ImmutableSet.of(Schema.Type.LONG, Schema.Type.STRING, Schema.Type.DOUBLE, Schema.Type.BOOLEAN, Schema.Type.BYTES,
                    Schema.Type.ARRAY, Schema.Type.RECORD);

  public static final String NAME_DATASET = "dataset";
  public static final String NAME_TABLE = "table";
  public static final String NAME_BUCKET = "bucket";
  public static final String NAME_SCHEMA = "schema";
  public static final String NAME_PARTITION_FROM = "partitionFrom";
  public static final String NAME_PARTITION_TO = "partitionTo";
  public static final String NAME_FILTER = "filter";
  public static final String NAME_ENABLE_QUERYING_VIEWS = "enableQueryingViews";
  public static final String NAME_VIEW_MATERIALIZATION_PROJECT = "viewMaterializationProject";
  public static final String NAME_VIEW_MATERIALIZATION_DATASET = "viewMaterializationDataset";
  public static final String NAME_USE_CONNECTION = "useConnection";
  public static final String NAME_CONNECTION = "connection";

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  public String referenceName;

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

  @Name(NAME_ENABLE_QUERYING_VIEWS)
  @Macro
  @Nullable
  @Description("Whether to allow querying views. Since BigQuery views are not materialized by default,"
    + " querying them may have a performance overhead.")
  private String enableQueryingViews;

  @Name(NAME_VIEW_MATERIALIZATION_PROJECT)
  @Macro
  @Nullable
  @Description("The project name where the view should be materialized. "
    + "Defaults to the same project in which the view is located.")
  private String viewMaterializationProject;

  @Name(NAME_VIEW_MATERIALIZATION_DATASET)
  @Macro
  @Nullable
  @Description("The dataset in the specified project where the view should be materialized. "
    + "Defaults to the same dataset in which the view is located.")
  private String viewMaterializationDataset;

  @Name(NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  private Boolean useConnection;

  @Name(NAME_CONNECTION)
  @Macro
  @Nullable
  @Description("The existing connection to use.")
  private BigQueryConnectorConfig connection;


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
      // remove the gs:// scheme from the bucket name
      if (bucket.startsWith(SCHEME)) {
        bucket = bucket.substring(SCHEME.length());
      }
      if (bucket.isEmpty()) {
        return null;
      }
    }
    return bucket;
  }

  public String getDatasetProject() {
    if (connection == null) {
      return ServiceOptions.getDefaultProjectId();
    }
    return connection.getDatasetProject();
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

  public void validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);
    String bucket = getBucket();

    if (!containsMacro(NAME_BUCKET)) {
      BigQueryUtil.validateBucket(bucket, NAME_BUCKET, collector);
    }

    if (!containsMacro(NAME_DATASET)) {
      BigQueryUtil.validateDataset(dataset, NAME_DATASET, collector);
    }

    if (!containsMacro(NAME_TABLE)) {
      validateTable(collector);
    }
  }

  private void validateTable(FailureCollector collector) {
    BigQueryUtil.validateTable(table, NAME_TABLE, collector);

    if (canConnect()) {
      Type definition = getSourceTableType();
      if (definition != null && definition == Type.VIEW && !isEnableQueryingViews()) {
        collector.addFailure(
          String.format("'%s' is a 'View' :", table),
          "In order to enable query views, please enable 'Enable Querying Views'");
      }
    }
  }

  /**
   * Reads table definition type from BigQuery
   *
   * @return {@link Type}
   */
  public Type getSourceTableType() {
    Table sourceTable =
      BigQueryUtil.getBigQueryTable(
        getDatasetProject(), getDataset(), table, getServiceAccount(), isServiceAccountFilePath());
    return sourceTable != null ? sourceTable.getDefinition().getType() : null;
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

  public boolean isEnableQueryingViews() {
    return "true".equalsIgnoreCase(enableQueryingViews);
  }

  @Nullable
  public String getViewMaterializationProject() {
    if (Strings.isNullOrEmpty(viewMaterializationProject)) {
      return getDatasetProject();
    }
    return viewMaterializationProject;
  }

  @Nullable
  public String getViewMaterializationDataset() {
    if (Strings.isNullOrEmpty(viewMaterializationDataset)) {
      return getDataset();
    }
    return viewMaterializationDataset;
  }

  /**
   * Returns true if bigquery table can be connected and schema is not a macro.
   */
  public boolean canConnect() {
    return !containsMacro(NAME_SCHEMA) && !containsMacro(NAME_DATASET) && !containsMacro(NAME_TABLE) &&
      connection != null && connection.canConnect();
  }

  /**
   * Return true if the service account is set to auto-detect but it can't be fetched from the environment.
   * This shouldn't result in a deployment failure, as the credential could be detected at runtime if the pipeline
   * runs on dataproc. This should primarily be used to check whether certain validation logic should be skipped.
   *
   * @return true if the service account is set to auto-detect but it can't be fetched from the environment.
   */
  public boolean autoServiceAccountUnavailable() {
    if (connection == null || connection.getServiceAccountFilePath() == null &&
      connection.isServiceAccountFilePath()) {
      try {
        ServiceAccountCredentials.getApplicationDefault();
      } catch (IOException e) {
        return true;
      }
    }
    return false;
  }

  public BigQueryConnectorConfig getConnection() {
    return connection;
  }
}
