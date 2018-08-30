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
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.gcs.GCPUtil;
import com.google.cloud.bigquery.Field;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableSchema;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This class <code>BigQuerySink</code> is a plugin that would allow users
 * to write <code>StructuredRecords</code> to Google Big Query.
 *
 * The plugin uses native BigQuery Output format to write data.
 */
@Plugin(type = "batchsink")
@Name(BigQuerySink.NAME)
@Description("This sink writes to a BigQuery table. "
  + "BigQuery is Google's serverless, highly scalable, enterprise data warehouse. "
  + "Data is first written to a temporary location on Google Cloud Storage, then loaded into BigQuery from there.")
public final class BigQuerySink extends BatchSink<StructuredRecord, JsonObject, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);
  public static final String NAME = "BigQueryTable";
  private BigQuerySinkConfig config;
  private Schema schema;
  private Configuration configuration;
  private JobID jobID = null;

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
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
    String projectId = GCPUtil.getProjectId(config.project);
    configuration.set("fs.gs.project.id", projectId);
    configuration.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);

    String temporaryGcsPath = String.format("gs://%s/hadoop/input/%s", config.bucket, uuid);
    configuration.set("fs.gs.system.bucket", config.bucket);
    configuration.setBoolean("fs.gs.impl.disable.cache", true);
    configuration.setBoolean("fs.gs.metadata.cache.enable", false);

    try {
      schema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse output schema. Reason: %s", e.getMessage()), e
      );
    }


    List<BigQueryTableFieldSchema> fields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      Schema.Type type = field.getSchema().getType();

      if (type == Schema.Type.RECORD) {
        continue;
      }

      String typeName = "STRING";
      if (type.isSimpleType()) {
        typeName = simpleType(field.getSchema());
      } else if (type == Schema.Type.UNION) {
        typeName = unionType(field.getSchema());
      } else if (type == Schema.Type.RECORD) {
        throw new IllegalArgumentException(
          String.format("Current implementation of BigQuery sink does not yet support complex records.")
        );
      }

      BigQueryTableFieldSchema fieldSchema = new BigQueryTableFieldSchema()
        .setName(name)
        .setType(typeName)
        .setMode(Field.Mode.NULLABLE.name());
      fields.add(fieldSchema);
    }

    BigQueryOutputConfiguration.configure(
      configuration,
      String.format("%s.%s", config.dataset, config.table),
      new BigQueryTableSchema().setFields(fields),
      temporaryGcsPath,
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
      TextOutputFormat.class);
    
    context.addOutput(Output.of(config.table.replace("-", "_").replace(".", "_"), new OutputFormatProvider() {
      @Override
      public String getOutputFormatClassName() {
        return IndirectBigQueryOutputFormat.class.getName();
      }

      @Override
      public Map<String, String> getOutputFormatConfiguration() {
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
      schema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse output schema. Reason: %s", e.getMessage()), e
      );
    }
  }

  String unionType(Schema schema) {
    for (Schema s : schema.getUnionSchemas()) {
      Schema.Type type = s.getType();
      if (type.isSimpleType()) {
        return simpleType(s);
      }
    }
    return "STRING";
  }

  String simpleType(Schema schema) {
    Schema.Type type = schema.getType();
    switch(type) {
      case INT:
        return "INTEGER";

      case STRING:
        return "STRING";

      case FLOAT:
      case DOUBLE:
        return "FLOAT";

      case BOOLEAN:
        return "BOOLEAN";

      case BYTES:
        return "BYTES";
    }
    return "STRING";
  }


  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<JsonObject, NullWritable>> emitter) throws Exception {
    List<Schema.Field> fields = schema.getFields();
    JsonObject object = new JsonObject();
    for (Schema.Field field : fields) {
      String name = field.getName();
      Object value = input.get(name);
      Schema.Type type = field.getSchema().getType();
      switch(type) {
        case INT:
        case LONG:
        case STRING:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
          decodeSimpleTypes(object, name, value, field.getSchema());
          break;

        case UNION:
          for (Schema schema : field.getSchema().getUnionSchemas()) {
            if (schema.getType().isSimpleType()) {
              decodeSimpleTypes(object, name, value, schema);
              break;
            }
          }
          break;
      }
    }
    emitter.emit(new KeyValue<>(object, NullWritable.get()));
  }

  private void decodeSimpleTypes(JsonObject json, String name,
                                 Object object, Schema schema) throws RecordConvertorException {
    Schema.Type type = schema.getType();

    if (object == null) {
      json.add(name, JsonNull.INSTANCE);
      return;
    }

    switch (type) {
      case NULL:
        json.add(name, JsonNull.INSTANCE); // nothing much to do here.
        break;

      case INT:
        if (object instanceof Integer || object instanceof Short) {
          json.addProperty(name, (Integer) object);
        } else if (object instanceof String) {
          String value = (String) object;
          try {
            json.addProperty(name, Integer.parseInt(value));
          } catch (NumberFormatException e) {
            throw new RecordConvertorException(
              String.format("Unable to convert '%s' to integer for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConvertorException(
            String.format("Schema specifies field '%s' is integer, but the value is not a integer or string. " +
                            "It is of type '%s'", name, object.getClass().getName())
          );
        }
        break;

      case LONG:
        if (object instanceof Long) {
          json.addProperty(name, (Long) object);
        } else if (object instanceof Integer) {
          json.addProperty(name, ((Integer) object).longValue());
        } else if (object instanceof Date) {
          json.addProperty(name, ((Date) object).getTime() / 1000); // Converts from milli-seconds to seconds.
        } else if (object instanceof java.sql.Date) {
          json.addProperty(name, ((java.sql.Date) object).getTime() / 1000);
        } else if (object instanceof Time) {
          json.addProperty(name, ((Time) object).getTime() / 1000);
        } else if (object instanceof Timestamp) {
          json.addProperty(name, ((Timestamp) object).getTime() / 1000);
        } else if (object instanceof Short) {
          json.addProperty(name, ((Short) object).longValue());
        } else if (object instanceof String) {
          String value = (String) object;
          try {
            json.addProperty(name, Long.parseLong(value));
          } catch (NumberFormatException e) {
            throw new RecordConvertorException(
              String.format("Unable to convert '%s' to long for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConvertorException(
            String.format("Schema specifies field '%s' is long, but the value is nor a string or long. " +
                            "It is of type '%s'", name, object.getClass().getName())
          );
        }
        break;

      case FLOAT:
        if (object instanceof Float) {
          json.addProperty(name, (Float) object);
        } else if (object instanceof Long) {
          json.addProperty(name, ((Long) object).floatValue());
        } else if (object instanceof Integer) {
          json.addProperty(name, ((Integer) object).floatValue());
        } else if (object instanceof Short) {
          json.addProperty(name, ((Short) object).floatValue());
        } else if (object instanceof String) {
          String value = (String) object;
          try {
            json.addProperty(name, Float.parseFloat(value));
          } catch (NumberFormatException e) {
            throw new RecordConvertorException(
              String.format("Unable to convert '%s' to float for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConvertorException(
            String.format("Schema specifies field '%s' is float, but the value is nor a string or float. " +
                            "It is of type '%s'", name, object.getClass().getName())
          );
        }
        break;

      case DOUBLE:
        if (object instanceof Double) {
          json.addProperty(name, (Double) object);
        } else if (object instanceof BigDecimal) {
          json.addProperty(name, ((BigDecimal) object).doubleValue());
        } else if (object instanceof Float) {
          json.addProperty(name, ((Float) object).doubleValue());
        } else if (object instanceof Long) {
          json.addProperty(name, ((Long) object).doubleValue());
        } else if (object instanceof Integer) {
          json.addProperty(name, ((Integer) object).doubleValue());
        } else if (object instanceof Short) {
          json.addProperty(name, ((Short) object).doubleValue());
        } else if (object instanceof String) {
          String value = (String) object;
          try {
            json.addProperty(name, Double.parseDouble(value));
          } catch (NumberFormatException e) {
            throw new RecordConvertorException(
              String.format("Unable to convert '%s' to double for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConvertorException(
            String.format("Schema specifies field '%s' is double, but the value is nor a string or double. " +
                            "It is of type '%s'", name, object.getClass().getName())
          );
        }
        break;

      case BOOLEAN:
        if (object instanceof Boolean) {
          json.addProperty(name, (Boolean) object);
        } else if (object instanceof String) {
          String value = (String) object;
          try {
            json.addProperty(name, Boolean.parseBoolean(value));
          } catch (NumberFormatException e) {
            throw new RecordConvertorException(
              String.format("Unable to convert '%s' to boolean for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConvertorException(
            String.format("Schema specifies field '%s' is double, but the value is nor a string or boolean. " +
                            "It is of type '%s'", name, object.getClass().getName())
          );
        }
        break;

      case STRING:
        json.addProperty(name, object.toString());
        break;
    }
  }
}
