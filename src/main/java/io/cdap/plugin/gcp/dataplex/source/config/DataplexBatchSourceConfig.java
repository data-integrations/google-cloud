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

package io.cdap.plugin.gcp.dataplex.source.config;

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.dataplex.common.config.DataplexBaseConfig;
import io.cdap.plugin.gcp.dataplex.common.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.common.exception.DataplexException;
import io.cdap.plugin.gcp.dataplex.common.model.Entity;
import io.cdap.plugin.gcp.dataplex.common.model.Lake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDate;

import javax.annotation.Nullable;

/**
 * Dataplex Source Plugin
 */
public class DataplexBatchSourceConfig extends DataplexBaseConfig {

  public static final String NAME_ENTITY = "entity";
  private static final Logger LOG = LoggerFactory.getLogger(DataplexBatchSourceConfig.class);
  private static final Gson GSON = new Gson();
  private static final String NAME_PARTITION_FROM = "partitionFrom";
  private static final String NAME_PARTITION_TO = "partitionTo";
  private static final String NAME_FILTER = "filter";
  private static final String NAME_SCHEMA = "schema";
  @Name(NAME_ENTITY)
  @Macro
  @Description("Resource id for the Dataplex entity. It represents a cloud resource that is being managed within a" +
    " lake as a member of a zone. User can type it in or press a browse button which enables " +
    "hierarchical selection.")
  protected String entity;

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
  @Name(NAME_SCHEMA)
  @Nullable
  @Macro
  @Description("The schema of the data to write. If provided, must be compatible with the table schema.")
  private String schema;

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

  public FileFormat getFormat(String format) {
    return FileFormat.from(format, FileFormat::canRead);
  }

  public Entity getAndValidateEntityConfiguration(FailureCollector collector, DataplexInterface dataplexInterface) {
    IdUtils.validateReferenceName(referenceName, collector);
    if (!Strings.isNullOrEmpty(location) && !containsMacro(NAME_LOCATION)) {
      try {
        dataplexInterface.getLocation(getCredentials(), tryGetProject(), location);
      } catch (DataplexException e) {
        configureDataplexException(location, NAME_LOCATION, e, collector);
        return null;
      }
      if (!Strings.isNullOrEmpty(lake) && !containsMacro(NAME_LAKE)) {
        try {
          dataplexInterface.getLake(getCredentials(), tryGetProject(), location, lake);
        } catch (DataplexException e) {
          configureDataplexException(lake, NAME_LAKE, e, collector);
          return null;
        }

        if (!Strings.isNullOrEmpty(zone) && !containsMacro(NAME_ZONE)) {
          try {
            dataplexInterface.getZone(getCredentials(), tryGetProject(), location, lake, zone);
          } catch (DataplexException e) {
            configureDataplexException(zone, NAME_ZONE, e, collector);
            return null;
          }
          if (!Strings.isNullOrEmpty(entity) && !containsMacro(NAME_ENTITY)) {
            try {
              Entity entityBean = dataplexInterface.getEntity(getCredentials(), tryGetProject(), location,
                lake, zone, entity);
              return entityBean;
            } catch (DataplexException e) {
              configureDataplexException(entity, NAME_ENTITY, e, collector);
              return null;
            }
          }
        }
      }
    }
    return null;
  }

  public void validateBigQueryDataset(FailureCollector collector, String project, String dataset, String tableName) {
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

  private void configureDataplexException(String dataplexConfigProperty, String dataplexConfigPropType,
                                          DataplexException e,
                                          FailureCollector failureCollector) {
    if (("404").equals(e.getCode())) {
      failureCollector
        .addFailure("'" + dataplexConfigProperty + "' could not be found. Please ensure that it exists in " +
          "Dataplex.", null).withConfigProperty(dataplexConfigPropType);
    } else {
      failureCollector.addFailure(e.getCode() + ": " + e.getMessage(), null)
        .withConfigProperty(dataplexConfigPropType);
    }
    failureCollector.getOrThrowException();
  }

  public void validateTable(FailureCollector collector, String project, String dataset, String tableId) {
    ConfigUtil.validateConnection(this, false, connection, collector);
    BigQueryUtil.validateTable(tableId, NAME_ENTITY, collector);
    if (containsMacro(NAME_LOCATION) || containsMacro(NAME_LAKE) || containsMacro(NAME_ZONE) ||
      containsMacro(NAME_ENTITY)) {
      return;
    }
    if (canConnect()) {
      TableDefinition.Type definition = getSourceTableType(project, dataset, tableId, collector);
      if (definition != null && definition == TableDefinition.Type.VIEW) {
        collector.addFailure(
          String.format("'%s' is a 'View' :", tableId),
          "In order to enable query views, please enable 'Enable Querying Views'");
      }
    }
  }

  /**
   * Returns true if bigquery table can be connected and schema is not a macro.
   */
  public boolean canConnect() {
    return !containsMacro(NAME_SCHEMA) &&
      connection != null && connection.canConnect();
  }

  /**
   * Reads table definition type from BigQuery
   *
   * @return {@link TableDefinition.Type}
   */
  public TableDefinition.Type getSourceTableType(String datasetProject, String dataSet,
                                                 String tableId, FailureCollector collector) {
    Table sourceTable =
      BigQueryUtil.getBigQueryTable(datasetProject, dataSet, tableId, getServiceAccount(), isServiceAccountFilePath());
    return sourceTable != null ? sourceTable.getDefinition().getType() : null;
  }

  public void setupValidatingInputFormat(PipelineConfigurer pipelineConfigurer, FailureCollector collector,
                                         Entity entity) {
    ConfigUtil.validateConnection(this, false, connection, collector);
    String fileFormat = FileFormat.CSV.toString().toLowerCase();
    collector.getOrThrowException();
    Schema schema = entity.getSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
    PluginProperties.Builder builder = PluginProperties.builder();
    builder.addAll(this.getRawProperties().getProperties());
    pipelineConfigurer.usePlugin("validatingInputFormat", fileFormat, fileFormat,
        builder.build());
  }

  public String getEntity() {
    return entity;
  }

  public void checkMetastoreForGCSEntity(DataplexInterface dataplexInterface, FailureCollector collector) {
    Lake lakeBean;
    if (!Strings.isNullOrEmpty(lake) && !containsMacro(NAME_LAKE)) {
      try {
        lakeBean = dataplexInterface.getLake(getCredentials(), tryGetProject(), location, lake);
        if (lakeBean.getMetastore() == null || (lakeBean.getMetastore() != null &&
          lakeBean.getMetastore().getService() == null)) {
          collector.addFailure(
            String.format("Metastore not attached with the lake '%s'.", lakeBean.getDisplayName()),
            "").withConfigProperty(NAME_LAKE);
        }
      } catch (DataplexException e) {
        configureDataplexException(lake, NAME_LAKE, e, collector);
      }
    }
  }
}

