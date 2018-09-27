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
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.gcp.common.GCPUtils;
import co.cask.hydrator.common.LineageRecorder;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableSchema;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

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
  public static final String NAME = "BigQueryTable";
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);
  private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  private static final Map<Schema.Type, LegacySQLTypeName> TYPE_MAP = ImmutableMap.<Schema.Type,
    LegacySQLTypeName>builder()
    .put(Schema.Type.INT, LegacySQLTypeName.INTEGER)
    .put(Schema.Type.LONG, LegacySQLTypeName.INTEGER)
    .put(Schema.Type.STRING, LegacySQLTypeName.STRING)
    .put(Schema.Type.FLOAT, LegacySQLTypeName.FLOAT)
    .put(Schema.Type.DOUBLE, LegacySQLTypeName.FLOAT)
    .put(Schema.Type.BOOLEAN, LegacySQLTypeName.BOOLEAN)
    .put(Schema.Type.BYTES, LegacySQLTypeName.BYTES)
    .build();

  private static final Map<Schema.LogicalType, LegacySQLTypeName> LOGICAL_TYPE_MAP = ImmutableMap.of(
    Schema.LogicalType.DATE, LegacySQLTypeName.DATE,
    Schema.LogicalType.TIME_MILLIS, LegacySQLTypeName.TIME, Schema.LogicalType.TIME_MICROS, LegacySQLTypeName.TIME,
    Schema.LogicalType.TIMESTAMP_MILLIS, LegacySQLTypeName.TIMESTAMP,
    Schema.LogicalType.TIMESTAMP_MICROS, LegacySQLTypeName.TIMESTAMP
  );

  private final BigQuerySinkConfig config;
  private Schema schema;
  private Configuration configuration;
  // UUID for the run. Will be used as bucket name if bucket is not provided.
  private UUID uuid;

  public BigQuerySink(BigQuerySinkConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    validateSchema();
    uuid = UUID.randomUUID();
    Job job = Job.getInstance();

    // some input formats require the credentials to be present in the job. We don't know for
    // sure which ones (HCatalog is one of them), so we simply always add them. This has no other
    // effect, because this method is only used at configure time and will be ignored later on.
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      job.getCredentials().addAll(credentials);
    }

    configuration = job.getConfiguration();
    configuration.clear();
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      configuration.set("mapred.bq.auth.service.account.json.keyfile", serviceAccountFilePath);
      configuration.set("google.cloud.auth.service.account.json.keyfile", serviceAccountFilePath);
    }
    configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configuration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    String projectId = config.getProject();
    configuration.set("fs.gs.project.id", projectId);
    configuration.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);

    String bucket = config.bucket;
    if (config.bucket == null) {
      bucket = uuid.toString();
      // By default, this option is false, meaning the job can not delete the bucket. So enable it only when bucket name
      // is not provided.
      configuration.setBoolean("fs.gs.bucket.delete.enable", true);
    }

    String temporaryGcsPath = String.format("gs://%s/hadoop/input/%s", bucket, uuid);
    configuration.set("fs.gs.system.bucket", bucket);
    configuration.setBoolean("fs.gs.impl.disable.cache", true);
    configuration.setBoolean("fs.gs.metadata.cache.enable", false);

    List<BigQueryTableFieldSchema> fields = new ArrayList<>();
    for (Schema.Field field : config.getSchema().getFields()) {
      String name = field.getName();
      Schema.Type type = field.getSchema().getType();

      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable()? fieldSchema.getNonNullable() : fieldSchema;
      if (!fieldSchema.getType().isSimpleType()) {
        throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'", name, type));
      }
      String tableTypeName = getTableDataType(fieldSchema);

      BigQueryTableFieldSchema tableFieldSchema = new BigQueryTableFieldSchema()
        .setName(name)
        .setType(tableTypeName)
        .setMode(Field.Mode.NULLABLE.name());
      fields.add(tableFieldSchema);
    }

    BigQueryOutputConfiguration.configure(
      configuration,
      String.format("%s.%s", config.dataset, config.table),
      new BigQueryTableSchema().setFields(fields),
      temporaryGcsPath,
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
      TextOutputFormat.class);

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());

    context.addOutput(Output.of(config.referenceName, new OutputFormatProvider() {
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

    if (!fields.isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to BigQuery table.",
                                  fields.stream().map(BigQueryTableFieldSchema::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    schema = config.getSchema();
  }

  private String getTableDataType(Schema schema) {
    Schema.LogicalType logicalType = schema.getLogicalType();

    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          return "DATE";
        case TIME_MILLIS:
        case TIME_MICROS:
          return "TIME";
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return "TIMESTAMP";
        default:
          throw new UnexpectedFormatException("Unsupported logical type " + logicalType);
      }
    }

    Schema.Type type = schema.getType();
    switch(type) {
      case INT:
      case LONG:
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
      default:
        throw new UnexpectedFormatException("Unsupported type " + type);
    }
  }


  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<JsonObject, NullWritable>> emitter) throws Exception {
    List<Schema.Field> fields = config.getSchema().getFields();
    JsonObject object = new JsonObject();
    for (Schema.Field field : fields) {
      String name = field.getName();
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      Schema.Type type = fieldSchema.getType();

      if (!type.isSimpleType()) {
        throw new RecordConverterException(String.format("Field '%s' is of unsupported type '%s'.", name, type));
      }

      decodeSimpleTypes(object, name, input, fieldSchema);
    }
    emitter.emit(new KeyValue<>(object, NullWritable.get()));
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    if (config.bucket == null) {
      Path gcsPath = new Path(String.format("gs://%s", uuid.toString()));
      try {
        FileSystem fs = gcsPath.getFileSystem(configuration);
        if (fs.exists(gcsPath)) {
          fs.delete(gcsPath, true);
        }
      } catch (IOException e) {
        LOG.warn("Failed to delete bucket " + gcsPath.toUri().getPath() + ", " + e.getMessage());
      }
    }
  }

  private void decodeSimpleTypes(JsonObject json, String name, StructuredRecord input, Schema schema)
    throws RecordConverterException {
    Object object = input.get(name);

    if (object == null) {
      json.add(name, JsonNull.INSTANCE);
      return;
    }

    Schema.LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          json.addProperty(name, input.getDate(name).toString());
          break;
        case TIME_MILLIS:
        case TIME_MICROS:
          json.addProperty(name, input.getTime(name).toString());
          break;
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          //timestamp for json input should be in this format yyyy-MM-dd HH:mm:ss.SSSSSS
          json.addProperty(name, dtf.format(input.getTimestamp(name)));
          break;
        default:
          throw new RecordConverterException(String.format("Unsupported logical type %s", logicalType));
      }
      return;
    }

    Schema.Type type = schema.getType();
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
            throw new RecordConverterException(
              String.format("Unable to convert '%s' to integer for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConverterException(
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
        } else if (object instanceof Short) {
          json.addProperty(name, ((Short) object).longValue());
        } else if (object instanceof String) {
          String value = (String) object;
          try {
            json.addProperty(name, Long.parseLong(value));
          } catch (NumberFormatException e) {
            throw new RecordConverterException(
              String.format("Unable to convert '%s' to long for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConverterException(
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
            throw new RecordConverterException(
              String.format("Unable to convert '%s' to float for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConverterException(
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
            throw new RecordConverterException(
              String.format("Unable to convert '%s' to double for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConverterException(
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
            throw new RecordConverterException(
              String.format("Unable to convert '%s' to boolean for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConverterException(
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

  /**
   * Validates output schema against bigquery table schema. It throws {@link IllegalArgumentException}
   * if the output schema has more fields than bigquery table or output schema field types does not match bigquery
   * column types.
   */
  private void validateSchema() throws IOException {
    Table table = getBigqueryTable(config);
    if (table == null) {
      // Table does not exist, so no further validation is required.
      return;
    }

    com.google.cloud.bigquery.Schema bqSchema = table.getDefinition().getSchema();
    if (bqSchema == null) {
      // Table is created without schema, so no further validation is required.
      return;
    }

    FieldList bqFields = bqSchema.getFields();
    List<Schema.Field> outputSchemaFields = config.getSchema().getFields();

    // Output schema should not have fields that are not present in BigQuery table.
    List<String> diff = getFieldDiff(bqFields, outputSchemaFields);
    if (!diff.isEmpty()) {
      throw new IllegalArgumentException(
        String.format("The output schema does not match the BigQuery table schema for '%s.%s' table. " +
                        "The table does not contain the '%s' column(s).", config.dataset, config.table, diff));
    }

    // validate the missing columns in output schema are nullable fields in bigquery
    List<String> remainingBQFields = getFieldDiff(outputSchemaFields, bqFields);
    for (String field : remainingBQFields) {
      if (bqFields.get(field).getMode() != Field.Mode.NULLABLE) {
        throw new IllegalArgumentException(
          String.format("The output schema does not match the BigQuery table schema for '%s.%s'. " +
                          "The table requires column '%s', which is not in the output schema.",
                        config.dataset, config.table, field));
      }
    }

    // Match output schema field types with bigquery column types
    matchSchema(config, bqFields, outputSchemaFields);
  }

  private Table getBigqueryTable(BigQuerySinkConfig config) throws IOException {
    BigQuery bigquery;
    BigQueryOptions.Builder bigqueryBuilder = BigQueryOptions.newBuilder();
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      bigqueryBuilder.setCredentials(GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath));
    }
    String project = config.getProject();
    bigqueryBuilder.setProjectId(project);
    bigquery = bigqueryBuilder.build().getService();

    TableId id = TableId.of(project, config.dataset, config.table);
    return bigquery.getTable(id);
  }

  private List<String> getFieldDiff(FieldList bqFields, List<Schema.Field> outputSchemaFields) {
    List<String> diff = new ArrayList<>();

    for (Schema.Field field : outputSchemaFields) {
      diff.add(field.getName());
    }

    for (Field field : bqFields) {
      diff.remove(field.getName());
    }
    return diff;
  }

  private List<String> getFieldDiff(List<Schema.Field> outputSchemaFields, FieldList bqFields) {
    List<String> diff = new ArrayList<>();

    for (Field field : bqFields) {
      diff.add(field.getName());
    }

    for (Schema.Field field : outputSchemaFields) {
      diff.remove(field.getName());
    }
    return diff;
  }

  private void matchSchema(BigQuerySinkConfig config, FieldList bqFields, List<Schema.Field> outputSchemaFields) {
    // validate type of fields against bigquery column types
    for (Schema.Field field : outputSchemaFields) {
      Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
      Schema.Type type = fieldSchema.getType();
      Schema.LogicalType logicalType = fieldSchema.getLogicalType();

      // validate logical types
      if (logicalType != null) {
        if (LOGICAL_TYPE_MAP.get(logicalType) == null) {
          throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'",
                                                           field.getName(), logicalType));
        }
        if (LOGICAL_TYPE_MAP.get(logicalType) != bqFields.get(field.getName()).getType()) {
          throw new IllegalArgumentException(
            String.format("Field '%s' of type '%s' is not compatible with column '%s' in BigQuery table" +
                            " '%s.%s' of type '%s'. It must be of type '%s'.",
                          field.getName(), logicalType, bqFields.get(field.getName()).getName(), config.dataset,
                          config.table, bqFields.get(field.getName()).getType(),
                          bqFields.get(field.getName()).getType()));
        }
        // return once logical types are validated. This is because logical types are represented as primitive types
        // internally.
        return;
      }

      if (TYPE_MAP.get(type) == null) {
        throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'",
                                                         field.getName(), type));
      }

      if (TYPE_MAP.get(type) != bqFields.get(field.getName()).getType()) {
        throw new IllegalArgumentException(
          String.format("Field '%s' of type '%s' is not compatible with column '%s' in BigQuery table" +
                          " '%s.%s' of type '%s'. It must be of type '%s'.",
                        field.getName(), type, bqFields.get(field.getName()).getName(), config.dataset,
                        config.table, bqFields.get(field.getName()).getType(),
                        bqFields.get(field.getName()).getType()));
      }
    }
  }
}
