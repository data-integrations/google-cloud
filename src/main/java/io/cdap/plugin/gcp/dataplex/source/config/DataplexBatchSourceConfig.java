/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.gcp.dataplex.source.config;

import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.dataplex.v1.DataplexServiceClient;
import com.google.cloud.dataplex.v1.Entity;
import com.google.cloud.dataplex.v1.EntityName;
import com.google.cloud.dataplex.v1.GetEntityRequest;
import com.google.cloud.dataplex.v1.Lake;
import com.google.cloud.dataplex.v1.LakeName;
import com.google.cloud.dataplex.v1.MetadataServiceClient;
import com.google.cloud.dataplex.v1.ZoneName;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.InvalidPluginProperty;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.config.DataplexBaseConfig;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexUtil;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Dataplex Source Plugin
 */
public class DataplexBatchSourceConfig extends DataplexBaseConfig {

  private static final String NAME_ENTITY = "entity";
  private static final String NAME_PARTITION_FROM = "partitionFrom";
  private static final String NAME_PARTITION_TO = "partitionTo";
  private static final String NAME_FILTER = "filter";
  private static final String NAME_SCHEMA = "schema";
  public static final String INPUT_FORMAT = "avro";
  @Name(NAME_ENTITY)
  @Macro
  @Description("ID of the Dataplex entity, which can be found on the entity detail page.")
  protected String entity;

  @Name(NAME_PARTITION_FROM)
  @Macro
  @Nullable
  @Description("Inclusive partition start date. Must be a string with format ‘yyyy-MM-dd’." +
    " The value is ignored if the table does not support partitioning.")
  private final String partitionFrom;
  @Name(NAME_PARTITION_TO)
  @Macro
  @Nullable
  @Description("Inclusive partition end date. Must be a string with format ‘yyyy-MM-dd’." +
    " The value is ignored if the table does not support partitioning.")
  private final String partitionTo;
  @Name(NAME_FILTER)
  @Macro
  @Nullable
  @Description("The WHERE clause filters out rows by evaluating each row against boolean expression, " +
    "and discards all rows that do not return TRUE (that is, rows that return FALSE or NULL).")
  private String filter;
  @Name(NAME_SCHEMA)
  @Nullable
  @Macro
  @Description("The schema of the data to write. If provided, must be compatible with the table schema.")
  private final String schema;

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
    }
    return filter;
  }

  /**
   * Parse json and return CDAP specific schema
   *
   * @param collector
   * @return Schema
   */
  @Nullable
  public Schema getSchema(FailureCollector collector) {
    if (Strings.isNullOrEmpty(schema)) {
      return null;
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null).
        withConfigProperty(NAME_SCHEMA);
    }
    // if there was an error that was added, it will throw an exception, otherwise,
    // this statement will not be executed
    throw collector.getOrThrowException();
  }

  /**
   * Validate location, lake ,zone and entity properties and return entity.
   *
   * @param collector FailureCollector
   * @return Entity fetched from dataplex system.
   */
  public Entity getAndValidateEntityConfiguration(FailureCollector collector,
                                                  GoogleCredentials credentials) throws IOException {
    if (!Strings.isNullOrEmpty(referenceName)) {
      IdUtils.validateReferenceName(referenceName, collector);
    }
    String projectID = tryGetProject();
    if (!Strings.isNullOrEmpty(location) && !containsMacro(NAME_LOCATION)) {
      if (!Strings.isNullOrEmpty(lake) && !containsMacro(NAME_LAKE)) {
        try (DataplexServiceClient dataplexServiceClient =
               DataplexUtil.getDataplexServiceClient(credentials)) {
          dataplexServiceClient.getLake(LakeName.newBuilder().setProject(projectID).setLocation(location)
            .setLake(lake).build());
        } catch (ApiException e) {
          // No methods provided for location validation, so validating based on Error message from dataplex.
          if (e.getMessage().contains("Location")) {
            configureDataplexException(location, NAME_LOCATION, e, collector);
          } else {
            configureDataplexException(lake, NAME_LAKE, e, collector);
          }
          return null;
        }

        if (!Strings.isNullOrEmpty(zone) && !containsMacro(NAME_ZONE)) {
          try (DataplexServiceClient dataplexServiceClient =
                 DataplexUtil.getDataplexServiceClient(credentials)) {
            dataplexServiceClient.getZone(ZoneName.of(projectID, location, lake, zone).toString());
          } catch (ApiException e) {
            configureDataplexException(zone, NAME_ZONE, e, collector);
            return null;
          }
          if (!Strings.isNullOrEmpty(entity) && !containsMacro(NAME_ENTITY)) {
            try (MetadataServiceClient metadataServiceClient =
                   DataplexUtil.getMetadataServiceClient(credentials)) {
              Entity entityBean =
                metadataServiceClient.getEntity(GetEntityRequest.newBuilder().setName(EntityName.of(projectID,
                  location, lake, zone, entity).toString()).setView(GetEntityRequest.EntityView.FULL).build());
              return entityBean;
            } catch (ApiException e) {
              configureDataplexException(entity, NAME_ENTITY, e, collector);
              return null;
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * If entity is of type BigQuery, validate it's properties
   *
   * @param collector FailureCollector
   * @param project   projectName
   * @param dataset   dataset name
   * @param tableName Table name
   */
  public void validateBigQueryDataset(FailureCollector collector, String project, String dataset, String tableName) {
    BigQueryUtil.validateTable(tableName, NAME_ENTITY, collector);
    Table sourceTable = BigQueryUtil.getBigQueryTable(project, dataset, tableName, this.getServiceAccount(),
      this.isServiceAccountFilePath(), collector);
    if (sourceTable == null) {
      return;
    }
    if (sourceTable.getDefinition() instanceof StandardTableDefinition) {
      TimePartitioning timePartitioning = ((StandardTableDefinition) sourceTable.getDefinition()).getTimePartitioning();
      if (timePartitioning == null) {
        return;
      }
    }

    String partitionFromDate = getPartitionFrom();
    String partitionToDate = getPartitionTo();

    if (partitionFromDate == null && partitionToDate == null) {
      return;
    }
    LocalDate fromDate = null;
    if (partitionFromDate != null) {
      try {
        fromDate = LocalDate.parse(partitionFromDate);
      } catch (DateTimeException ex) {
        collector.addFailure("Invalid partition from date format.",
            "Ensure partition from date is of format 'yyyy-MM-dd'.")
          .withConfigProperty(BigQuerySourceConfig.NAME_PARTITION_FROM);
      }
    }
    LocalDate toDate = null;
    if (partitionToDate != null) {
      try {
        toDate = LocalDate.parse(partitionToDate);
      } catch (DateTimeException ex) {
        collector.addFailure("Invalid partition to date format.", "Ensure partition to date is of format 'yyyy-MM-dd'.")
          .withConfigProperty(BigQuerySourceConfig.NAME_PARTITION_TO);
      }
    }

    if (fromDate != null && toDate != null && fromDate.isAfter(toDate) && !fromDate.isEqual(toDate)) {
      collector.addFailure("'Partition From Date' must be before or equal 'Partition To Date'.", null)
        .withConfigProperty(BigQuerySourceConfig.NAME_PARTITION_FROM)
        .withConfigProperty(BigQuerySourceConfig.NAME_PARTITION_TO);
    }
  }

  /**
   * Returns true if bigquery table can be connected and schema is not a macro.
   */
  private boolean canConnect() {
    return !containsMacro(NAME_SCHEMA) &&
      connection != null && connection.canConnect();
  }

  /**
   * Reads table definition type from BigQuery
   *
   * @return {@link TableDefinition.Type}
   */
  public TableDefinition.Type getSourceTableType(String datasetProject, String dataSet,
                                                 String tableId) {
    Table sourceTable =
      BigQueryUtil.getBigQueryTable(datasetProject, dataSet, tableId, getServiceAccount(), isServiceAccountFilePath());
    return sourceTable != null ? sourceTable.getDefinition().getType() : null;
  }


  /**
   * Setting validating input format as avro as we are setting same format in task output format.
   *
   * @param pipelineConfigurer
   * @param collector
   * @param entity             Dataplex entity
   */
  public void setupValidatingInputFormat(PipelineConfigurer pipelineConfigurer, FailureCollector collector,
                                         @Nullable Entity entity) {
    String fileFormat = INPUT_FORMAT;
    collector.getOrThrowException();
    if (entity != null) {
      if (this.getSchema(collector) == null) {
        Schema schema = DataplexUtil.getTableSchema(entity.getSchema(), collector);
        pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
      }
    }
    PluginProperties.Builder builder = PluginProperties.builder();
    builder.addAll(this.getRawProperties().getProperties());
    pipelineConfigurer.usePlugin("validatingInputFormat", fileFormat, fileFormat,
      builder.build());
  }

  /**
   * Return default file System properties for cloud storage
   *
   * @param path
   * @return
   */
  public Map<String, String> getFileSystemProperties(String path) {
    Map<String, String> properties = GCPUtils.getFileSystemProperties(this.getConnection(),
      path, new HashMap<>());
    return properties;
  }

  /**
   * Validate the schema based on validating input format
   *
   * @param context
   * @param fileFormat
   * @param validatingInputFormat
   */
  private void validateInputFormatProvider(FormatContext context, String fileFormat,
                                           @Nullable ValidatingInputFormat validatingInputFormat) {
    FailureCollector collector = context.getFailureCollector();
    if (validatingInputFormat == null) {
      collector.addFailure(String.format("Could not find the '%s' input format.", fileFormat), null)
        .withPluginNotFound(fileFormat, fileFormat, "validatingInputFormat");
    } else {
      validatingInputFormat.validate(context);
    }

  }

  /**
   * @param batchSourceContext
   * @return
   * @throws InstantiationException
   */
  public ValidatingInputFormat getValidatingInputFormat(BatchSourceContext batchSourceContext)
    throws InstantiationException {
    FailureCollector collector = batchSourceContext.getFailureCollector();
    String fileFormat = INPUT_FORMAT;
    ValidatingInputFormat validatingInputFormat;
    try {
      validatingInputFormat = batchSourceContext.newPluginInstance(fileFormat);
    } catch (InvalidPluginConfigException exception) {
      Set<String> properties = new HashSet(exception.getMissingProperties());
      Iterator iterator = exception.getInvalidProperties().iterator();

      while (iterator.hasNext()) {
        InvalidPluginProperty invalidProperty = (InvalidPluginProperty) iterator.next();
        properties.add(invalidProperty.getName());
      }
      String errorMessage = String.format(
        "Format '%s' cannot be used because properties %s were not provided or were invalid when the pipeline was " +
          "deployed. Set the format to a different value, or re-create the pipeline with all required properties.",
        fileFormat, properties);
      throw new IllegalArgumentException(errorMessage, exception);
    }
    FormatContext formatContext = new FormatContext(collector, batchSourceContext.getInputSchema());
    this.validateInputFormatProvider(formatContext, fileFormat, validatingInputFormat);
    collector.getOrThrowException();
    return validatingInputFormat;
  }

  public String getEntity() {
    return entity;
  }

  /**
   * check if dataproc metastore is attached to the lake, otherwise throw Exception
   *
   * @param collector
   */
  public void checkMetastoreForGCSEntity(FailureCollector collector, GoogleCredentials credentials) {
    Lake lakeBean;
    if (!Strings.isNullOrEmpty(lake) && !containsMacro(NAME_LAKE)) {
      try (DataplexServiceClient dataplexServiceClient =
             DataplexUtil.getDataplexServiceClient(credentials)) {
        lakeBean = dataplexServiceClient.getLake(LakeName.newBuilder().setProject(tryGetProject()).
          setLocation(location).
          setLake(lake).build());
        if (lakeBean.getMetastore() == null || (lakeBean.getMetastore() != null &&
          lakeBean.getMetastore().getService() == null)) {
          collector.addFailure(
            String.format("Metastore not attached with the lake '%s'.", lakeBean.getDisplayName()),
            "").withConfigProperty(NAME_LAKE);
        }
      } catch (ApiException e) {
        configureDataplexException(lake, NAME_LAKE, e, collector);
      } catch (IOException e) {
        collector.addFailure(e.getMessage(), "Please check credentials");
      }
      collector.getOrThrowException();
    }
  }

  public static DataplexBatchSourceConfig.Builder builder() {
    return new DataplexBatchSourceConfig.Builder();
  }

  private DataplexBatchSourceConfig(String entity, String schema, String location, String lake, String zone,
                                    GCPConnectorConfig connection, @Nullable String referenceName, String partitionTo,
                                    String partitionFrom, String filter) {
    this.entity = entity;
    this.schema = schema;
    this.location = location;
    this.lake = lake;
    this.zone = zone;
    this.connection = connection;
    this.referenceName = referenceName;
    this.partitionTo = partitionTo;
    this.partitionFrom = partitionFrom;
    this.filter = filter;
  }

  /**
   * Dataplex Batch Source configuration builder.
   */

  public static class Builder {
    private String entity;
    private String schema;
    private String location;
    private String lake;
    private String zone;
    private GCPConnectorConfig connection;
    private String referenceName;
    private String partitionTo;
    private String partitionFrom;
    private String filter;

    public Builder setEntity(String entity) {
      this.entity = entity;
      return this;
    }

    public Builder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public Builder setLocation(String location) {
      this.location = location;
      return this;
    }

    public Builder setLake(String lake) {
      this.lake = lake;
      return this;
    }

    public Builder setZone(String zone) {
      this.zone = zone;
      return this;
    }

    public Builder setConnection(GCPConnectorConfig connection) {
      this.connection = connection;
      return this;
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setPartitionTo(String partitionTo) {
      this.partitionTo = partitionTo;
      return this;
    }

    public Builder setpartitionFrom(String partitionFrom) {
      this.partitionFrom = partitionFrom;
      return this;
    }

    public Builder setFilter(String filter) {
      this.filter = filter;
      return this;
    }

    public DataplexBatchSourceConfig build() {
      return new DataplexBatchSourceConfig(this.entity, schema, location, lake, zone, connection, referenceName,
        partitionTo, partitionFrom, filter);
    }
  }
}
