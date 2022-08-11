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

import com.google.auth.Credentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition.Type;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Storage;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Holds configuration required for configuring {@link BigQuerySource}.
 */
public final class BigQuerySourceConfig extends BigQueryBaseConfig {
  private static final String VALID_DATE_FORMAT = "yyyy-MM-dd";
  private static final String SCHEME = "gs://";
  private static final String WHERE = "WHERE";
  public static final Set<Schema.Type> SUPPORTED_TYPES =
    ImmutableSet.of(Schema.Type.LONG, Schema.Type.STRING, Schema.Type.DOUBLE, Schema.Type.BOOLEAN, Schema.Type.BYTES,
                    Schema.Type.ARRAY, Schema.Type.RECORD);

  public static final String NAME_TABLE = "table";
  public static final String NAME_SCHEMA = "schema";
  public static final String NAME_PARTITION_FROM = "partitionFrom";
  public static final String NAME_PARTITION_TO = "partitionTo";
  public static final String NAME_FILTER = "filter";
  public static final String NAME_ENABLE_QUERYING_VIEWS = "enableQueryingViews";
  public static final String NAME_VIEW_MATERIALIZATION_PROJECT = "viewMaterializationProject";
  public static final String NAME_VIEW_MATERIALIZATION_DATASET = "viewMaterializationDataset";

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  public String referenceName;


  @Name(NAME_TABLE)
  @Macro
  @Description("The table to read from. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  private String table;

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
  @Description("The project name where the temporary table should be created. "
    + "Defaults to the same project in which the table is located.")
  private String viewMaterializationProject;

  @Name(NAME_VIEW_MATERIALIZATION_DATASET)
  @Macro
  @Nullable
  @Description("The dataset in the specified project where the temporary table should be created. "
    + "Defaults to the same dataset in which the table is located.")
  private String viewMaterializationDataset;

  public String getTable() {
    return table;
  }

  public String getDatasetProject() {
    if (connection == null) {
      return ServiceOptions.getDefaultProjectId();
    }
    return connection.getDatasetProject();
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

    if (!containsMacro(NAME_DATASET)) {
      BigQueryUtil.validateDataset(dataset, NAME_DATASET, collector);
    }

    if (null != partitionFrom) {
      validatePartitionDate(collector, partitionFrom, NAME_PARTITION_FROM);
    }

    if (null != partitionTo) {
      validatePartitionDate(collector, partitionTo, NAME_PARTITION_TO);
    }

    if (!containsMacro(NAME_TABLE)) {
      validateTable(collector);
    }
    if (!containsMacro(NAME_CMEK_KEY)) {
      validateCmekKey(collector, arguments);
    }
  }

  void validateCmekKey(FailureCollector collector, Map<String, String> arguments) {
    CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(cmekKey, arguments, collector);
    //these fields are needed to check if bucket exists or not and for location validation
    if (cmekKeyName == null || !canConnect() || containsMacro(NAME_BUCKET)) {
      return;
    }
    DatasetId datasetId = DatasetId.of(getDatasetProject(), getDataset());
    Credentials credentials = connection.getCredentials(collector);
    BigQuery bigQuery = GCPUtils.getBigQuery(getProject(), credentials);
    if (bigQuery == null) {
      return;
    }
    Dataset dataset = null;
    try {
      dataset = bigQuery.getDataset(datasetId);
    } catch (Exception e) {
      //If there is an exception getting dataset information during validation, it will be ignored.
      return;
    }
    Storage storage = GCPUtils.getStorage(getProject(), credentials);
    if (dataset == null || storage == null) {
      return;
    }
    GCSPath gcsPath = Strings.isNullOrEmpty(bucket) ? null : GCSPath.from(bucket);
    CmekUtils.validateCmekKeyAndBucketLocation(storage, gcsPath, cmekKeyName, dataset.getLocation(), collector);
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

  private void validatePartitionDate(FailureCollector collector, String partitionDate, String fieldName) {
    SimpleDateFormat date = new SimpleDateFormat(VALID_DATE_FORMAT);
    date.setLenient(false);
    try {
      date.parse(partitionDate);
    } catch (ParseException e) {
      collector.addFailure(String.format("%s is not in a valid format.", partitionDate),
                           String.format("Enter valid date in format: %s", VALID_DATE_FORMAT))
      .withConfigProperty(fieldName);
    }
  }

  /**
   * Reads table definition type from BigQuery
   *
   * @return {@link Type}
   */
  public Type getSourceTableType() {
    Table sourceTable =
      BigQueryUtil.getBigQueryTable(getDatasetProject(), getDataset(), table, getServiceAccount(),
                                    isServiceAccountFilePath());
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

    private BigQuerySourceConfig(@Nullable BigQueryConnectorConfig connection, @Nullable String dataset,
                                @Nullable String cmekKey, @Nullable String bucket, @Nullable String table) {
      super(connection, dataset, cmekKey, bucket);
      this.table = table;
    }

  public static Builder builder() {
     return new Builder();
  }

  /**
   * BigQuery Source configuration builder.
   */
  public static class Builder {
    private BigQueryConnectorConfig connection;
    private String dataset;
    private String cmekKey;
    private String bucket;
    private String table;

    public Builder setConnection(@Nullable BigQueryConnectorConfig connection) {
      this.connection = connection;
      return this;
    }

    public Builder setDataset(@Nullable String dataset) {
      this.dataset = dataset;
      return this;
    }

    public Builder setTable(@Nullable String table) {
      this.table = table;
      return this;
    }

    public Builder setCmekKey(@Nullable String cmekKey) {
      this.cmekKey = cmekKey;
      return this;
    }

    public Builder setBucket(@Nullable String bucket) {
      this.bucket = bucket;
      return this;
    }

    public BigQuerySourceConfig build() {
      return new BigQuerySourceConfig(connection, dataset, cmekKey, bucket, table);
    }
  }
}
