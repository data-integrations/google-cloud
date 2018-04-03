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

package co.cask.bigquery;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.gson.JsonObject;
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.Path;

/**
 * Class description here.
 */
@Plugin(type = "batchsource")
@Name(BigQuerySource.NAME)
@Description(BigQuerySource.DESCRIPTION)
public final class BigQuerySource extends BatchSource<LongWritable, JsonObject, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySource.class);
  public static final String NAME = "BigQueryTable";
  public static final String DESCRIPTION = "Reads from Google BigQuery Table";
  private BigQuerySourceConfig config;
  private Schema outputSchema;
  private GoogleCredentials credentials;
  private Configuration configuration;
  private JobID jobID = null;
  private final RecordConvertor convertor = new RecordConvertor();

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    if(!config.containsMacro("serviceFilePath")) {
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

    configuration.set("mapred.bq.auth.service.account.json.keyfile", config.serviceAccountFilePath);
    configuration.set("google.cloud.auth.service.account.json.keyfile", config.serviceAccountFilePath);
    configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configuration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    configuration.set("fs.gs.project.id", config.project);
    configuration.set("fs.gs.system.bucket", config.bucket);
    configuration.setBoolean("fs.gs.impl.disable.cache", true);
    configuration.setBoolean("fs.gs.metadata.cache.enable", false);
    configuration.set(BigQueryConfiguration.PROJECT_ID_KEY, config.project);

    String temporaryGcsPath = String.format("gs://%s/hadoop/output/%s", config.bucket, uuid);
    GsonBigQueryInputFormat.setTemporaryCloudStorageDirectory(configuration, temporaryGcsPath);
    GsonBigQueryInputFormat.setEnableShardedExport(configuration, false);
    BigQueryConfiguration.configureBigQueryInput(configuration, config.project, config.dataset, config.table);
    BigQueryConfiguration.getTemporaryPathRoot(configuration, jobID);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    context.setInput(Input.of(config.table, new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return GsonBigQueryInputFormat.class.getName();
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
  public void transform(KeyValue<LongWritable, JsonObject> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    emitter.emit(convertor.toSr(input.getValue(), outputSchema));
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
      GsonBigQueryInputFormat.cleanupJob(configuration, jobID);
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
    GoogleCredentials credentials = loadServiceAccountCredentials(request.serviceFilePath);
    try {
      BigQuery bigquery;
      bigquery = BigQueryOptions.newBuilder()
        .setProjectId(request.project)
        .setCredentials(credentials)
        .build()
        .getService();


      TableId id = TableId.of(request.project, request.dataset, request.table);
      Table table = bigquery.getTable(id);
      com.google.cloud.bigquery.Schema bgSchema = table.getDefinition().getSchema();

      List<Schema.Field> fields = new ArrayList<>();
      for (Field field : bgSchema.getFields()) {
        Field.Type type = field.getType();
        Schema.Type cType = Schema.Type.STRING;
        LegacySQLTypeName value = type.getValue();
        if (value == LegacySQLTypeName.FLOAT) {
          cType = Schema.Type.FLOAT;
        } else if (value == LegacySQLTypeName.BOOLEAN) {
          cType = Schema.Type.BOOLEAN;
        } else if (value == LegacySQLTypeName.INTEGER) {
          cType = Schema.Type.INT;
        } else if (value == LegacySQLTypeName.STRING) {
          cType = Schema.Type.STRING;
        } else if (value == LegacySQLTypeName.BYTES) {
          cType = Schema.Type.BYTES;
        } else if (value == LegacySQLTypeName.TIME) {
          cType = Schema.Type.STRING;
        } else if (value == LegacySQLTypeName.DATE) {
          cType = Schema.Type.STRING;
        } else if (value == LegacySQLTypeName.DATETIME) {
          cType = Schema.Type.STRING;
        } else if (value == LegacySQLTypeName.RECORD) {
          throw new Exception("Nested records not supported yet.");
        }

        if (field.getMode() == null || field.getMode() == Field.Mode.NULLABLE) {
          Schema fieldType = Schema.nullableOf(Schema.of(cType));
          fields.add(Schema.Field.of(field.getName(), fieldType));
        } else if (field.getMode() == Field.Mode.REQUIRED) {
          fields.add(Schema.Field.of(field.getName(), Schema.of(cType)));
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

  public static ServiceAccountCredentials loadServiceAccountCredentials(String path) throws Exception {
    File credentialsPath = new File(path);
    if (!credentialsPath.exists()) {
      throw new FileNotFoundException("Service account file " + credentialsPath.getName() + " does not exist.");
    }
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    } catch (FileNotFoundException e) {
      throw new Exception(
        String.format("Unable to find service account file '%s'.", path)
      );
    } catch (IOException e) {
      throw new Exception(
        String.format(
          "Issue reading service account file '%s', please check permission of the file", path
        )
      );
    }
  }

}
