/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.gcp.bigquery;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.gcp.common.AvroToStructuredTransformer;
import co.cask.gcp.common.GCPUtils;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.AvroBigQueryInputFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.Path;

import static co.cask.gcp.common.GCPUtils.loadServiceAccountCredentials;

/**
 * Class description here.
 */
@Plugin(type = "batchsource")
@Name(BigQuerySource.NAME)
@Description("This source reads the entire contents of a BigQuery table. "
  + "BigQuery is Google's serverless, highly scalable, enterprise data warehouse."
  + "Data is first written to a temporary location on Google Cloud Storage, then read into the pipeline from there.")
public final class BigQuerySource extends BatchSource<LongWritable, GenericData.Record, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySource.class);
  public static final String NAME = "BigQueryTable";
  private BigQuerySourceConfig config;
  private Schema outputSchema;
  private Configuration configuration;
  private JobID jobID = null;
  private final AvroToStructuredTransformer transformer = new AvroToStructuredTransformer();

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    if (!config.containsMacro("serviceFilePath") && config.serviceAccountFilePath != null) {
      File file = new File(config.serviceAccountFilePath);
      if (!file.exists()) {
        throw new IllegalArgumentException(
          String.format("Service account file '%s' does not exist in the path specified.", file.getName())
        );
      }
    }
    if (!config.containsMacro("schema")) {
      try {
        outputSchema = Schema.parseJson(config.schema);
        configurer.getStageConfigurer().setOutputSchema(outputSchema);
      } catch (IOException e) {
        throw new IllegalArgumentException(
          String.format("Unable to parse output schema. Reason: %s", e.getMessage()), e
        );
      }
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = Job.getInstance();

    // some input formats require the credentials to be present in the job. We don't know for
    // sure which ones (HCatalog is one of them), so we simply always add them. This has no other
    // effect, because this method is only used at configure time and will be ignored later on.
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      job.getCredentials().addAll(credentials);
    }

    // Construct a unique job id
    String uuid = UUID.randomUUID().toString();
    jobID = JobID.forName(String.format("job_%s-%s-%s_%s", context.getNamespace(),
                                        context.getPipelineName().replaceAll("_", "-"), uuid, 1));

    configuration = job.getConfiguration();
    configuration.clear();

    if (config.serviceAccountFilePath != null) {
      configuration.set("mapred.bq.auth.service.account.json.keyfile", config.serviceAccountFilePath);
      configuration.set("google.cloud.auth.service.account.json.keyfile", config.serviceAccountFilePath);
    }
    configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configuration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    String projectId = GCPUtils.getProjectId(config.project);
    configuration.set("fs.gs.project.id", projectId);
    configuration.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);

    configuration.set("fs.gs.system.bucket", config.bucket);
    configuration.setBoolean("fs.gs.impl.disable.cache", true);
    configuration.setBoolean("fs.gs.metadata.cache.enable", false);

    String temporaryGcsPath = String.format("gs://%s/hadoop/output/%s", config.bucket, uuid);

    AvroBigQueryInputFormat.setTemporaryCloudStorageDirectory(configuration, temporaryGcsPath);
    AvroBigQueryInputFormat.setEnableShardedExport(configuration, false);
    BigQueryConfiguration.configureBigQueryInput(configuration, config.project, config.dataset, config.table);
    BigQueryConfiguration.getTemporaryPathRoot(configuration, jobID);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    context.setInput(Input.of(config.table, new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return AvroBigQueryInputFormat.class.getName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        Map<String, String> config = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration) {
          config.put(entry.getKey(), entry.getValue());
        }
        return config;
      }
    }));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    try {
      outputSchema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse output schema. Reason: %s", e.getMessage()), e
      );
    }
  }

  /**
   * Converts <code>JsonObject</code> to <code>StructuredRecord</code> for every record
   * retrieved from the BigQuery table.
   *
   * @param input input record
   * @param emitter emitting the transformed record into downstream nodes.
   */
  @Override
  public void transform(KeyValue<LongWritable, GenericData.Record> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    emitter.emit(transformer.transform(input.getValue(), outputSchema));
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    super.onRunFinish(succeeded, context);
    try {
      org.apache.hadoop.fs.Path tempPath = new org.apache.hadoop.fs.Path(
        ConfigurationUtil.getMandatoryConfig(
          configuration, BigQueryConfiguration.TEMP_GCS_PATH_KEY));
      try {
        FileSystem fs = tempPath.getFileSystem(configuration);
        if (fs.exists(tempPath)) {
          LOG.info("Deleting temp GCS input path '{}'", tempPath);
          fs.delete(tempPath, true);
        }
      } catch (IOException e) {
        // Error is swallowed as job has completed successfully and the only failure is deleting
        // temporary data.
        // This matches the FileOutputCommitter pattern.
        LOG.warn("Could not delete intermediate GCS files. Temporary data not cleaned up.", e);
      }
      AvroBigQueryInputFormat.cleanupJob(configuration, jobID);
    } catch (IOException e) {
      LOG.warn("There was issue shutting down BigQuery job. " + e.getMessage());
    }
  }

  /**
   * This method retrieves the schema of the bigquery table and translates it into
   * CDAP schema. The schemas type support for BigQuery and CDAP are almost the same,
   * hence, the translation is not that complicated.
   *
   * @param request Received from the UI with the configuration for project, dataset, table and service account file.
   * @return Translated schema.
   * @throws Exception
   */
  @Path("getSchema")
  public Schema getSchema(Request request) throws Exception {
    try {
      BigQuery bigquery;
      BigQueryOptions.Builder bigqueryBuilder = BigQueryOptions.newBuilder();
      if (request.serviceFilePath != null) {
        bigqueryBuilder.setCredentials(loadServiceAccountCredentials(request.serviceFilePath));
      }
      String project = request.project == null ? ServiceOptions.getDefaultProjectId() : request.project;
      if (project == null) {
        throw new Exception("Could not detect Google Cloud project id from the environment. " +
                              "Please specify a project id.");
      }
      bigqueryBuilder.setProjectId(project);
      bigquery = bigqueryBuilder.build().getService();

      TableId id = TableId.of(project, request.dataset, request.table);
      Table table = bigquery.getTable(id);
      com.google.cloud.bigquery.Schema bgSchema = table.getDefinition().getSchema();

      List<Schema.Field> fields = new ArrayList<>();
      for (Field field : bgSchema.getFields()) {
        LegacySQLTypeName type = field.getType();
        Schema schema;
        StandardSQLTypeName value = type.getStandardType();
        if (value == StandardSQLTypeName.FLOAT64) {
          // float is a float64, so corresponding type becomes double
          schema = Schema.of(Schema.Type.DOUBLE);
        } else if (value == StandardSQLTypeName.BOOL) {
          schema = Schema.of(Schema.Type.BOOLEAN);
        } else if (value == StandardSQLTypeName.INT64) {
          // int is a int64, so corresponding type becomes long
          schema = Schema.of(Schema.Type.LONG);
        } else if (value == StandardSQLTypeName.STRING) {
          schema = Schema.of(Schema.Type.STRING);
        } else if (value == StandardSQLTypeName.BYTES) {
          schema = Schema.of(Schema.Type.BYTES);
        } else if (value == StandardSQLTypeName.TIME) {
          schema = Schema.of(Schema.LogicalType.TIME_MICROS);
        } else if (value == StandardSQLTypeName.DATE) {
          schema = Schema.of(Schema.LogicalType.DATE);
        } else if (value == StandardSQLTypeName.TIMESTAMP || value == StandardSQLTypeName.DATETIME) {
          schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        } else {
          // this should never happen
          throw new UnsupportedTypeException(String.format("Big Query Sql type %s is not supported", value));
        }

        if (field.getMode() == null || field.getMode() == Field.Mode.NULLABLE) {
          fields.add(Schema.Field.of(field.getName(), Schema.nullableOf(schema)));
        } else if (field.getMode() == Field.Mode.REQUIRED) {
          fields.add(Schema.Field.of(field.getName(), schema));
        } else if (field.getMode() == Field.Mode.REPEATED) {
          throw new UnsupportedTypeException("Repeated type is not supported");
        }
      }
      return Schema.recordOf("output", fields);
    } catch (Exception e) {
      throw new Exception(e.getMessage(), e);
    }
  }

  /**
   * Request object for retrieving schema from the BigQuery table.
   */
  class Request {
    // Specifies the master list of servers.
    public String project;

    // Name of dataset
    public String dataset;

    // Name of the table.
    public String table;

    // Service account file path.
    public String serviceFilePath;
  }
}
