/*
 *
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

package io.cdap.plugin.gcp.bigquery.connector;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * BigQuery Connector Plugin
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(BigQueryConnector.NAME)
@Description("This connector creates connections to BigQuery, browses BigQuery datasets and tables, sample BigQuery " +
  "tables. BigQuery is Google's serverless, highly scalable, enterprise data warehouse.")
public final class BigQueryConnector implements DirectConnector {
  public static final String NAME = "BigQuery";
  public static final String ENTITY_TYPE_DATASET = "dataset";
  public static final String ENTITY_TYPE_TABLE = "table";
  private BigQueryConnectorConfig config;

  @Override
  public List<StructuredRecord> sample(SampleRequest sampleRequest) throws IOException {
    String path = sampleRequest.getPath();
    String[] parts = parsePath(path);
    if (parts.length != 2) {
      throw new IllegalArgumentException(
        String.format("Path %s is invalid. Should contain dataset and table name.", path));
    }
    String dataset = parts[0];
    if (dataset.isEmpty()) {
      throw new IllegalArgumentException(String.format("Path %s is invalid. Dataset name is empty.", path));
    }
    String table = parts[1];
    if (table.isEmpty()) {
      throw new IllegalArgumentException(String.format("Path %s is invalid. Table name is empty.", path));
    }

    return getData(getBigQuery(), dataset, table, sampleRequest.getLimit());
  }

  @Override
  public void test(FailureCollector failureCollector) throws ValidationException {
    // Should not happen
    if (config == null) {
      failureCollector.addFailure("Connector config is null!", "Please instantiate connector with a valid config.");
      return;
    }
    // validate project ID
    String project = config.tryGetProject();
    if (project == null) {
      failureCollector
        .addFailure("Could not detect Google Cloud project id from the environment.", "Please specify a project id.");
    }

    GoogleCredentials credentials = null;
    //validate service account
    if (config.isServiceAccountJson() || config.getServiceAccountFilePath() != null) {
      try {
        credentials =
          GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(), config.isServiceAccountFilePath());
      } catch (Exception e) {
        failureCollector.addFailure(String.format("Service account key provided is not valid: %s.", e.getMessage()),
          "Please provide a valid service account key.");
      }
    }
    // if either project or credentials cannot be loaded , no need to continue
    if (!failureCollector.getValidationFailures().isEmpty()) {
      return;
    }

    try {
      BigQuery bigQuery = GCPUtils.getBigQuery(config.getDatasetProject(), credentials);
      bigQuery.listDatasets(BigQuery.DatasetListOption.pageSize(1));
    } catch (Exception e) {
      failureCollector.addFailure(String.format("Could not connect to BigQuery: %s", e.getMessage()),
        "Please specify correct connection properties.");
    }
  }


  @Override
  public BrowseDetail browse(BrowseRequest browseRequest) throws IOException {
    String path = browseRequest.getPath();
    String[] parts = parsePath(path);

    BigQuery bigQuery = getBigQuery();
    if ("/".equals(path) || path.isEmpty()) {
      // browse project to list all datasets
      return listDatasets(bigQuery, browseRequest.getLimit());
    }
    String dataset = parts[0];
    if (parts.length == 1) {
      // browse project to list all tables
      if (dataset.isEmpty()) {
        throw new IllegalArgumentException(String.format("Path %s is invalid. Dataset name is empty.", path));
      }
      return listTables(bigQuery, dataset, browseRequest.getLimit());
    }
    String table = parts[1];
    if (table.isEmpty()) {
      throw new IllegalArgumentException(String.format("Path %s is invalid. Table name is empty.", path));
    }
    return getTable(bigQuery, dataset, table);
  }

  private BrowseDetail getTable(BigQuery bigQuery, String dataset, String table) {
    Table result = bigQuery.getTable(TableId.of(dataset, table));
    if (result == null) {
      throw new IllegalArgumentException(String.format("Cannot find such table: %s.%s.", dataset, table));
    }
    return BrowseDetail.builder()
      .addEntity(BrowseEntity.builder(table, "/" + dataset + "/" + table, ENTITY_TYPE_TABLE).canSample(true).build())
      .build();
  }

  private String[] parsePath(String originalPath) {
    String path = originalPath;
    if (originalPath.startsWith("/")) {
      path = originalPath.substring(1);
    }
    String[] parts = path.split("/");
    // path should contains at most two part : dataset and table
    if (parts.length > 2) {
      throw new IllegalArgumentException(
        String.format("Path %s is invalid. Path should at most contain two parts.", originalPath));
    }
    return parts;
  }

  private BrowseDetail listTables(BigQuery bigQuery, String dataset, Integer limit) {
    int countLimit = limit == null ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    DatasetId datasetId = DatasetId.of(dataset);
    Page<Table> tablePage = bigQuery.listTables(datasetId);
    String parentPath = "/" + dataset + "/";
    for (Table table : tablePage.iterateAll()) {
      String name = table.getTableId().getTable();
      browseDetailBuilder
        .addEntity(BrowseEntity.builder(name, parentPath + name, ENTITY_TYPE_TABLE).canSample(true).build());
      count++;
      if (count == countLimit) {
        break;
      }
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  private BrowseDetail listDatasets(BigQuery bigQuery, Integer limit) {
    Page<Dataset> datasetPage = bigQuery.listDatasets(BigQuery.DatasetListOption.all());
    int countLimit = limit == null ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    for (Dataset dataset : datasetPage.iterateAll()) {
      String name = dataset.getDatasetId().getDataset();
      browseDetailBuilder
        .addEntity(BrowseEntity.builder(name, "/" + name, ENTITY_TYPE_DATASET).canBrowse(true).build());
      count++;
      if (count == countLimit) {
        break;
      }
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  private BigQuery getBigQuery() throws IOException {
    GoogleCredentials credentials = null;
    //validate service account
    if (config.isServiceAccountJson() || config.getServiceAccountFilePath() != null) {
      credentials =
        GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(), config.isServiceAccountFilePath());
    }
    ;
    return GCPUtils.getBigQuery(config.getDatasetProject(), credentials);
  }

  private List<StructuredRecord> getData(BigQuery bigQuery, String dataset, String table, int limit)
    throws IOException {
    String query = String.format("SELECT * FROM `%s.%s.%s` LIMIT %d", config.getDatasetProject(), dataset, table,
      limit);
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
    // Wait for the job to finish
    try {
      queryJob = queryJob.waitFor();
    } catch (InterruptedException e) {
      throw new IOException("Query job interrupted.", e);
    }

    // check for errors
    if (queryJob == null) {
      throw new IOException("Job no longer exists.");
    } else if (queryJob.getStatus().getError() != null) {
      throw new IOException(String.format("Failed to query table : %s", queryJob.getStatus().getError().toString()));
    }

    // Get the results
    TableResult result = null;
    try {
      result = queryJob.getQueryResults();
    } catch (InterruptedException e) {
      throw new IOException("Query results interrupted.", e);
    }
    List<StructuredRecord> samples = new ArrayList<>();

    com.google.cloud.bigquery.Schema schema = result.getSchema();
    Schema cdapSchema = BigQueryUtil.getTableSchema(schema, null);

    FieldList fields = schema.getFields();
    for (FieldValueList fieldValues : result.iterateAll()) {
      StructuredRecord.Builder recordBuilder = StructuredRecord.builder(cdapSchema);
      for (Field field : fields) {
        String fieldName = field.getName();
        FieldValue fieldValue = fieldValues.get(fieldName);
        FieldValue.Attribute attribute = fieldValue.getAttribute();
        LegacySQLTypeName type = field.getType();
        StandardSQLTypeName standardType = type.getStandardType();
        if (fieldValue.isNull()) {
          recordBuilder.set(fieldName, null);
          continue;
        }

        if (attribute == FieldValue.Attribute.REPEATED) {
          List<Object> list = new ArrayList<>();
          for (FieldValue value : fieldValue.getRepeatedValue()) {
            list.add(getValue(standardType, value));
          }
          recordBuilder.set(fieldName, list);
        } else {
          Object value = getValue(standardType, fieldValue);
          if (value instanceof ZonedDateTime) {
            recordBuilder.setTimestamp(fieldName, (ZonedDateTime) value);
          } else if (value instanceof LocalTime) {
            recordBuilder.setTime(fieldName, (LocalTime) value);
          } else if (value instanceof LocalDate) {
            recordBuilder.setDate(fieldName, (LocalDate) value);
          } else if (value instanceof LocalDateTime) {
            recordBuilder.setDateTime(fieldName, (LocalDateTime) value);
          } else if (value instanceof BigDecimal) {
            recordBuilder.setDecimal(fieldName, (BigDecimal) value);
          } else {
            recordBuilder.set(fieldName, value);
          }
        }
      }
      samples.add(recordBuilder.build());
    }
    return samples;
  }

  private Object getValue(StandardSQLTypeName standardType, FieldValue fieldValue) {
    switch (standardType) {
      case TIME:
        return LocalTime.parse(fieldValue.getStringValue());
      case DATE:
        return LocalDate.parse(fieldValue.getStringValue());
      case TIMESTAMP:
        long tsMicroValue = fieldValue.getTimestampValue();
        return getZonedDateTime(tsMicroValue);
      case NUMERIC:
        BigDecimal decimal = fieldValue.getNumericValue();
        if (decimal.scale() < 9) {
          // scale up the big decimal. this is because structured record expects scale to be exactly same as schema
          // Big Query supports maximum unscaled value up to 38 digits. so scaling up should still be <= max
          // precision
          decimal = decimal.setScale(9);
        }
        return decimal;

      case DATETIME:
        return LocalDateTime.parse(fieldValue.getStringValue());
      case STRING:
        return fieldValue.getStringValue();
      case BOOL:
        return fieldValue.getBooleanValue();
      case FLOAT64:
        return fieldValue.getDoubleValue();
      case INT64:
        return fieldValue.getLongValue();
      case BYTES:
        return fieldValue.getBytesValue();
      default:
        throw new RuntimeException(String.format("BigQuery type %s is not supported.", standardType));
    }
  }

  private ZonedDateTime getZonedDateTime(long microTs) {
    long tsInSeconds = TimeUnit.MICROSECONDS.toSeconds(microTs);
    long mod = TimeUnit.MICROSECONDS.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (microTs % mod);
    Instant instant = Instant.ofEpochSecond(tsInSeconds, TimeUnit.MICROSECONDS.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

  @Override
  public ConnectorSpec generateSpec(ConnectorSpecRequest connectorSpecRequest) {
    String path = connectorSpecRequest.getPath();
    String[] parts = parsePath(path);
    if (parts.length != 2) {
      throw new IllegalArgumentException(
        String.format("Path %s is invalid. Should contain dataset and table name.", path));
    }
    return ConnectorSpec.builder().addProperty(BigQuerySourceConfig.NAME_DATASET, parts[0])
      .addProperty(BigQuerySourceConfig.NAME_TABLE, parts[1]).build();
  }
}
