package io.cdap.plugin.gcp.dataplex.source.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.config.DataplexBaseConfig;
import io.cdap.plugin.gcp.dataplex.sink.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.sink.connector.DataplexConnectorConfig;
import io.cdap.plugin.gcp.dataplex.sink.exception.ConnectorException;
import io.cdap.plugin.gcp.dataplex.sink.model.Asset;
import io.cdap.plugin.gcp.dataplex.sink.model.Zone;
import io.cdap.plugin.gcp.gcs.GCSPath;

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

  public static final String NAME_PATH = "path";
  private static final Logger LOG = LoggerFactory.getLogger(DataplexBatchSourceConfig.class);
  private static final String SKIP_HEADER = "skipHeader";
  private static final String NAME_PARTITION_FROM = "partitionFrom";
  private static final String NAME_PARTITION_TO = "partitionTo";
  private static final String NAME_FILTER = "filter";
  private static final String NAME_USE_CONNECTION = "useConnection";
  private static final String NAME_CONNECTION = "connection";
  private static final String NAME_SCHEMA = "schema";
  private static final String NAME_TABLE = "table";
  private static final String NAME_FORMAT = "format";
  private static final String NAME_DELIMITER = "delimiter";
  @Name(SKIP_HEADER)
  @Nullable
  @Macro
  @Description("Whether to skip the first line of each file. Supported only for CSV format.")
  protected Boolean skipHeader;

  @Name(NAME_PARTITION_FROM)
  @Macro
  @Nullable
  @Description("It's inclusive partition start date. It should be a String with format \"yyyy-MM-dd\". " +
    "This value is ignored if the table does not support partitioning.")
  private String partitionFrom;

  @Name(NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  private Boolean useConnection;

  @Name(NAME_CONNECTION)
  @Nullable
  @Macro
  @Description("The existing connection to use.")
  private DataplexConnectorConfig connection;

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

  @Name(NAME_TABLE)
  @Macro
  @Description("The table to read from. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  private String table;

  @Name(NAME_SCHEMA)
  @Nullable
  @Macro
  @Description("The schema of the data to write. If provided, must be compatible with the table schema.")
  private String schema;

  @Name(NAME_FORMAT)
  @Macro
  @Description("Format of the data to read. Supported formats are 'avro', 'blob', 'csv', 'delimited', 'json', "
    + "'parquet', 'text', and 'tsv'.")
  private String format;

  @Name(NAME_PATH)
  @Macro
  @Description("The path to read from. For example, gs://<bucket>/path/to/directory/")
  private String path;

  @Macro
  @Nullable
  @Description("The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the format "
    + "is anything other than 'delimited'.")
  private String delimiter;

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
  public String getServiceAccountType() {
    return connection == null ? null : connection.getServiceAccountType();
  }

  @Nullable
  public Boolean isTruncateTable() {
    return skipHeader != null && skipHeader;
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
    }
    return filter;
  }

  @Nullable
  public Boolean isServiceAccountFilePath() {
    return connection == null ? null : connection.isServiceAccountFilePath();
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

  public String getTable() {
    return table;
  }

  public FileFormat getFormat() {
    return FileFormat.from(format, FileFormat::canRead);
  }

  public String getPath() {
    return path;
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

  public void validateServiceAccount(FailureCollector failureCollector) {
    if (connection.isServiceAccountJson() || connection.getServiceAccountFilePath() != null) {
      try {
        GoogleCredentials credentials = getCredentials();
      } catch (Exception e) {
        failureCollector.addFailure(String.format("Service account key provided is not valid: %s.",
            e.getMessage()), "Please provide a valid service account key.").withConfigProperty("serviceFilePath")
          .withConfigProperty("serviceAccountJSON");
      }
    }
    failureCollector.getOrThrowException();
  }

  public GoogleCredentials getCredentials() {
    GoogleCredentials credentials = null;
    try {
      //validate service account
      if (connection.isServiceAccountJson() || connection.getServiceAccountFilePath() != null) {
        credentials =
          GCPUtils.loadServiceAccountCredentials(connection.getServiceAccount(), connection.isServiceAccountFilePath())
            .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
      }
    } catch (IOException e) {
      LOG.debug("Unable to load service account credentials due to error: {}", e.getMessage());
    }
    return credentials;
  }

  public DataplexConnectorConfig getConnection() {
    return connection;
  }

  public void validateAssetConfiguration(FailureCollector collector, DataplexInterface dataplexInterface) {
    IdUtils.validateReferenceName(referenceName, collector);
    if (!Strings.isNullOrEmpty(location) && !containsMacro(NAME_LOCATION)) {
      try {
        dataplexInterface.getLocation(getCredentials(), tryGetProject(), location);
      } catch (ConnectorException e) {
        configureDataplexException(location, NAME_LOCATION, e, collector);
        return;
      }
      if (!Strings.isNullOrEmpty(lake) && !containsMacro(NAME_LAKE)) {
        try {
          dataplexInterface.getLake(getCredentials(), tryGetProject(), location, lake);
        } catch (ConnectorException e) {
          configureDataplexException(lake, NAME_LAKE, e, collector);
          return;
        }

        if (!Strings.isNullOrEmpty(zone) && !containsMacro(NAME_ZONE)) {
          Zone zoneBean = null;
          try {
            zoneBean = dataplexInterface.getZone(getCredentials(), tryGetProject(), location, lake, zone);
          } catch (ConnectorException e) {
            configureDataplexException(zone, NAME_ZONE, e, collector);
            return;
          }
          if (!Strings.isNullOrEmpty(asset) && !containsMacro(NAME_ASSET)) {
            try {
              Asset assetBean = dataplexInterface.getAsset(getCredentials(), tryGetProject(), location,
                lake, zone, asset);
              if (!assetType.equalsIgnoreCase(assetBean.getAssetResourceSpec().getType())) {
                collector.addFailure("Asset type doesn't match with actual asset. ", null).
                  withConfigProperty(NAME_ASSET_TYPE);
              }
            } catch (ConnectorException e) {
              configureDataplexException(asset, NAME_ASSET, e, collector);
              return;
            }
          }
        }
      }
    }
    collector.getOrThrowException();
  }

  public void validateBigQueryDataset(FailureCollector collector) {
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
                                          ConnectorException e,
                                          FailureCollector failureCollector) {
    if (("404").equals(e.getCode())) {
      failureCollector
        .addFailure("'" + dataplexConfigProperty + "' could not be found. Please ensure that it exists in " +
          "Dataplex.", null).withConfigProperty(dataplexConfigPropType);
    } else {
      failureCollector.addFailure(e.getCode() + ": " + e.getMessage(), null)
        .withConfigProperty(dataplexConfigPropType);
    }
  }

  public void validateTable(FailureCollector collector) {
    BigQueryUtil.validateTable(table, NAME_TABLE, collector);
  }

  public void validateGCS(FailureCollector collector) {
    if (!containsMacro(NAME_FORMAT)) {
      try {
        getFormat();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_FORMAT)
          .withStacktrace(e.getStackTrace());
      }
    }

    if (!containsMacro(NAME_PATH)) {
      try {
        GCSPath.from(path);
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_PATH)
          .withStacktrace(e.getStackTrace());
      }
    }
  }

}
