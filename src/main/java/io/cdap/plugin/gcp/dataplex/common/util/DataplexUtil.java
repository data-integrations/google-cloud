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

package io.cdap.plugin.gcp.dataplex.common.util;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.Field.Mode;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.common.connection.impl.DataplexInterfaceImpl;
import io.cdap.plugin.gcp.dataplex.common.exception.DataplexException;
import io.cdap.plugin.gcp.dataplex.common.model.Field;
import io.cdap.plugin.gcp.dataplex.common.model.Job;
import io.cdap.plugin.gcp.dataplex.sink.DataplexBatchSink;
import io.cdap.plugin.gcp.dataplex.source.DataplexBatchSource;

import org.apache.hadoop.conf.Configuration;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Common Util class for dataplex plugins such as {@link DataplexBatchSource} and {@link DataplexBatchSink}
 */
public final class DataplexUtil {

  private static final Logger LOG = LoggerFactory.getLogger(DataplexUtil.class);

  /**
   * Converts Entity Schema into a CDAP Schema object.
   *
   * @param dataplexSchema Dataplex Schema to be converted.
   * @param collector      Failure collector to collect failure messages for the client.
   * @return CDAP schema object
   */
  public static Schema getTableSchema(io.cdap.plugin.gcp.dataplex.common.model.Schema dataplexSchema,
                                      @Nullable FailureCollector collector) {
    List<Field> fields = dataplexSchema.getFields();
    if (dataplexSchema.getPartitionFields() != null && !dataplexSchema.getPartitionFields().isEmpty()) {
      fields.addAll(dataplexSchema.getPartitionFields());
    }
    List<Schema.Field> schemafields = new ArrayList<>();

    for (Field field : fields) {
      Schema.Field schemaField = getSchemaField(field, collector, null);
      // if schema field is null, that means that there was a validation error. We will still continue in order to
      // collect more errors
      if (schemaField == null) {
        continue;
      }
      schemafields.add(schemaField);
    }
    if (schemafields.isEmpty() && collector != null && !collector.getValidationFailures().isEmpty()) {
      // throw if there was validation failure(s) added to the collector
      collector.getOrThrowException();
    }
    if (schemafields.isEmpty()) {
      return null;
    }
    return Schema.recordOf("output", schemafields);
  }

  /**
   * Converts Entity schema field into a corresponding CDAP Schema.Field.
   *
   * @param field        BigQuery field to be converted.
   * @param collector    Failure collector to collect failure messages for the client.
   * @param recordPrefix String to prepend to recordNames to make them unique
   * @return A CDAP schema field
   */
  @Nullable
  private static Schema.Field getSchemaField(Field field,
                                             @Nullable FailureCollector collector,
                                             @Nullable String recordPrefix) {
    Schema schema = convertFieldType(field, collector, recordPrefix);
    if (schema == null) {
      return null;
    }

    Mode mode =
      field.getMode() == null ? Mode.NULLABLE : field.getMode();
    switch (mode) {
      case NULLABLE:
        return Schema.Field.of(field.getName(), Schema.nullableOf(schema));
      case REQUIRED:
        return Schema.Field.of(field.getName(), schema);
      case REPEATED:
        return Schema.Field.of(field.getName(), Schema.arrayOf(schema));
      default:
        // this should not happen, unless newer bigquery versions introduces new mode that is not supported by this
        // plugin.
        String error = String.format("Field '%s' has unsupported mode '%s'.", field.getName(), mode);
        if (collector != null) {
          collector.addFailure(error, null);
        } else {
          throw new RuntimeException(error);
        }
    }
    return null;
  }

  /**
   * Converts Entity field type into a CDAP field type.
   *
   * @param field        Bigquery field to be converted.
   * @param collector    Failure collector to collect failure messages for the client.
   * @param recordPrefix String to add before a record name to ensure unique names.
   * @return A CDAP field schema
   */
  @Nullable
  private static Schema convertFieldType(Field field,
                                         @Nullable FailureCollector collector,
                                         @Nullable String recordPrefix) {
    DataplexTypeName standardType = field.getType();
    switch (standardType) {
      case FLOAT:
      case FLOAT64:
      case DOUBLE:
        // float is a float64, so corresponding type becomes double
        return Schema.of(Schema.Type.DOUBLE);
      case BOOL:
      case BOOLEAN:
        return Schema.of(Schema.Type.BOOLEAN);
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case LONG:
        return Schema.of(Schema.Type.LONG);
      case STRING:
        return Schema.of(Schema.Type.STRING);
      case DATETIME:
        return Schema.of(Schema.LogicalType.DATETIME);
      case BINARY:
      case BYTES:
        return Schema.of(Schema.Type.BYTES);
      case TIME:
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case DATE:
        return Schema.of(Schema.LogicalType.DATE);
      case TIMESTAMP:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case DECIMAL:
      case NUMERIC:
        // bigquery has Numeric.PRECISION digits of precision and Numeric.SCALE digits of scale for NUMERIC.
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
        return Schema.decimalOf(BigQueryTypeSize.Numeric.PRECISION, BigQueryTypeSize.Numeric.SCALE);
      case BIGNUMERIC:
        // bigquery has BigNumeric.PRECISION digits of precision and BigNumeric.SCALE digits of scale for BIGNUMERIC.
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
        return Schema.decimalOf(BigQueryTypeSize.BigNumeric.PRECISION, BigQueryTypeSize.BigNumeric.SCALE);
      case STRUCT:
        List<Field> fields = field.getSubFields();
        List<Schema.Field> schemaFields = new ArrayList<>();

        // Record names have to be unique as Avro doesn't allow to redefine them.
        // We can make them unique by prepending the previous records names to their name.
        String recordName = "";
        if (recordPrefix != null) {
          recordName = recordPrefix + '.';
        }
        recordName = recordName + field.getName();

        for (Field f : fields) {
          Schema.Field schemaField = getSchemaField(f, collector, recordName);
          // if schema field is null, that means that there was a validation error. We will still continue in order to
          // collect more errors
          if (schemaField == null) {
            continue;
          }
          schemaFields.add(schemaField);
        }
        // do not return schema for the struct field if none of the nested fields are of supported types
        if (!schemaFields.isEmpty()) {
          return Schema.recordOf(recordName, schemaFields);
        } else {
          return null;
        }
      default:
        String error =
          String.format("Entity column '%s' is of unsupported type '%s'.", field.getName(), standardType.name());
        String action = String.format("Supported column types are: %s.",
          BigQueryUtil.BQ_TYPE_MAP.keySet().stream().map(t -> t.getStandardType().name())
            .collect(Collectors.joining(", ")));
        if (collector != null) {
          collector.addFailure(error, action);
        } else {
          throw new RuntimeException(error + action);
        }
        return null;
    }
  }

  // To fetch the details whether job created by task creation is completed or not.
  public String getJobCompletion(Configuration conf) throws DataplexException, IOException {
    GoogleCredentials googleCredentials = getCredentialsFromConfiguration(conf);
    DataplexInterface dataplexInterface = new DataplexInterfaceImpl();
    String projectID = conf.get(DataplexConstants.DATAPLEX_PROJECT_ID);
    String location = conf.get(DataplexConstants.DATAPLEX_LOCATION);
    String lake = conf.get(DataplexConstants.DATAPLEX_LAKE);
    String taskId = conf.get(DataplexConstants.DATAPLEX_TASK_ID);
    List<Job> jobList = dataplexInterface.listJobs(googleCredentials,
      projectID, location, lake, taskId);
    Job dataplexJob = jobList.get(0);
    try {
      Awaitility.await()
        .atMost(30, TimeUnit.MINUTES)
        .pollInterval(15, TimeUnit.SECONDS)
        .pollDelay(5, TimeUnit.SECONDS)
        .until(() -> {
          Job currentJob = dataplexInterface.getJob(googleCredentials,
            projectID, location, lake, taskId, dataplexJob.getUid());
          LOG.debug("Job is still in running state");
          return currentJob.getState() != null && !"RUNNING".equalsIgnoreCase(currentJob.getState());
        });
    } catch (ConditionTimeoutException e) {
      throw new IllegalStateException("Job creation failed in dataproc.", e);
    }
    Job completedJob = dataplexInterface.getJob(googleCredentials,
      projectID, location, lake, taskId, dataplexJob.getUid());
    if (!"SUCCEEDED".equalsIgnoreCase(completedJob.getState())) {
      LOG.error(completedJob.getMessage());
      throw new IllegalStateException("Job failed in dataproc.");
    }
    String outputLocation = conf.get("mapreduce.input.fileinputformat.inputdir");
    outputLocation = outputLocation + completedJob.getName().replace("/jobs/", "/") + "/0/";
    conf.set("mapreduce.input.fileinputformat.inputdir", outputLocation);
    return completedJob.getName();
  }

  private GoogleCredentials getCredentialsFromConfiguration(Configuration configuration) throws IOException {
    String serviceAccount;
    String type = configuration.get("cdap.gcs.auth.service.account.type.flag");
    Boolean isServiceAccountJson = false;
    if (type == GCPConnectorConfig.SERVICE_ACCOUNT_JSON) {
      isServiceAccountJson = true;
      serviceAccount = configuration.get("google.cloud.auth.service.account.json");
    } else {
      serviceAccount = configuration.get("google.cloud.auth.service.account.json.keyfile");
    }
    String filePath = configuration.get("cdap.gcs.auth.service.account.type.filepath");
    return getCredentialsFromServiceAccount(isServiceAccountJson, filePath, serviceAccount);
  }

  private GoogleCredentials getCredentialsFromServiceAccount(Boolean isServiceAccountJson, String filePath,
                                                             String serviceAccount) throws IOException {
    GoogleCredentials credentials = null;
    if (isServiceAccountJson || (filePath != null && !filePath.equalsIgnoreCase("none"))) {
      credentials =
        GCPUtils.loadServiceAccountCredentials(serviceAccount, !isServiceAccountJson)
          .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
    } else {
      credentials = ServiceAccountCredentials.getApplicationDefault().createScoped(
        Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
    }
    return credentials;
  }

}
