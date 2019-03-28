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

package co.cask.gcp.spanner.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.api.validation.InvalidConfigPropertyException;
import co.cask.cdap.etl.api.validation.InvalidStageException;
import co.cask.gcp.common.Schemas;
import co.cask.gcp.spanner.SpannerConstants;
import co.cask.gcp.spanner.common.SpannerUtil;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.SourceInputFormatProvider;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Cloud Spanner batch source
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SpannerSource.NAME)
@Description("Batch source to read from Cloud Spanner. Cloud Spanner is a fully managed, mission-critical, " +
  "relational database service that offers transactional consistency at global scale, schemas, " +
  "SQL (ANSI 2011 with extensions), and automatic, synchronous replication for high availability.")
public class SpannerSource extends BatchSource<NullWritable, ResultSet, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerSource.class);
  private static final String TABLE_NAME = "TableName";
  // listing table's schema documented at https://cloud.google.com/spanner/docs/information-schema
  private static final Statement.Builder SCHEMA_STATEMENT_BUILDER =  Statement.newBuilder(
    String.format("SELECT  t.column_name,t.spanner_type, t.is_nullable FROM information_schema.columns AS t WHERE " +
                    "  t.table_catalog = ''  AND  t.table_schema = '' AND t.table_name = @%s", TABLE_NAME));
  public static final String NAME = "Spanner";
  private final SpannerSourceConfig config;
  private Schema schema;
  private Spanner spanner;

  public SpannerSource(SpannerSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    if (config.containsMacro("schema")) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
      return;
    }

    Schema schema = getSchema();
    Schema configuredSchema = config.getSchema();
    if (configuredSchema == null) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
      return;
    }

    try {
      Schemas.validateFieldsMatch(schema, configuredSchema);
      pipelineConfigurer.getStageConfigurer().setOutputSchema(configuredSchema);
    } catch (IllegalArgumentException e) {
      throw new InvalidConfigPropertyException(e.getMessage(), e, "schema");
    }
  }

  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) throws Exception {
    config.validate();
    String projectId = config.getProject();
    Configuration configuration = new Configuration();
    initializeConfig(configuration, projectId);

    // initialize spanner
    spanner = SpannerUtil.getSpannerService(config.getServiceAccountFilePath(), projectId);
    BatchClient batchClient =
      spanner.getBatchClient(DatabaseId.of(projectId, config.instance, config.database));
    Timestamp logicalStartTimeMicros =
      Timestamp.ofTimeMicroseconds(TimeUnit.MILLISECONDS.toMicros(batchSourceContext.getLogicalStartTime()));

    // create batch transaction id
    BatchReadOnlyTransaction batchReadOnlyTransaction =
      batchClient.batchReadOnlyTransaction(TimestampBound.ofReadTimestamp(logicalStartTimeMicros));
    BatchTransactionId batchTransactionId = batchReadOnlyTransaction.getBatchTransactionId();

    // partitionQuery returns ImmutableList which doesn't implement java Serializable interface,
    // we add to array list, which implements java Serializable
    List<Partition> partitions =
      new ArrayList<>(
        batchReadOnlyTransaction.partitionQuery(getPartitionOptions(),
                                                Statement.of(String.format("Select * from %s;", config.table))));

    // serialize batch transaction-id and partitions
    configuration.set(SpannerConstants.SPANNER_BATCH_TRANSACTION_ID, getSerializedObjectString(batchTransactionId));
    configuration.set(SpannerConstants.PARTITIONS_LIST, getSerializedObjectString(partitions));

    LineageRecorder lineageRecorder = new LineageRecorder(batchSourceContext, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());

    // set input format and pass configuration
    batchSourceContext.setInput(Input.of(config.referenceName,
                                         new SourceInputFormatProvider(SpannerInputFormat.class, configuration)));
    schema = batchSourceContext.getOutputSchema();
    if (schema != null) {
      if (schema.getFields() != null) {
        lineageRecorder.recordRead("Read", "Read from Spanner table.",
                                   config.getSchema().getFields().stream().map(Schema.Field::getName)
                                     .collect(Collectors.toList()));
      }
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    schema = context.getOutputSchema();
  }

  @Override
  public void transform(KeyValue<NullWritable, ResultSet> input, Emitter<StructuredRecord> emitter) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = schema.getFields();
    ResultSet resultSet = input.getValue();
    for (Schema.Field field : fields) {
      String fieldName = field.getName();
      Type columnType = resultSet.getColumnType(fieldName);
      if (columnType == null || resultSet.isNull(fieldName)) {
        continue;
      }
      switch (columnType.getCode()) {
        // todo CDAP-14233 - add support for array
        case BOOL:
          builder.set(fieldName, resultSet.getBoolean(fieldName));
          break;
        case INT64:
          builder.set(fieldName, resultSet.getLong(fieldName));
          break;
        case FLOAT64:
          builder.set(fieldName, resultSet.getDouble(fieldName));
          break;
        case STRING:
          builder.set(fieldName, resultSet.getString(fieldName));
          break;
        case BYTES:
          ByteArray byteArray = resultSet.getBytes(fieldName);
          builder.set(fieldName, byteArray.toByteArray());
          break;
        case DATE:
          // spanner DATE is a date without time zone. so create LocalDate from spanner DATE
          Date spannerDate = resultSet.getDate(fieldName);
          builder.setDate(fieldName, LocalDate.of(spannerDate.getYear(), spannerDate.getMonth(),
                                                  spannerDate.getDayOfMonth()));
          break;
        case TIMESTAMP:
          Timestamp spannerTs = resultSet.getTimestamp(fieldName);
          // Spanner TIMESTAMP supports nano second level precision, however, cdap schema only supports
          // microsecond level precision.
          Instant instant = Instant.ofEpochSecond(spannerTs.getSeconds()).plusNanos(spannerTs.getNanos());
          builder.setTimestamp(fieldName, ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
          break;
      }
    }
    emitter.emit(builder.build());
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    LOG.info("Run finished, closing spanner client");
    // free up spanner resources
    if (spanner != null) {
      spanner.close();
    }
  }

  private void initializeConfig(Configuration configuration, String projectId) {
    setIfValueNotNull(configuration, SpannerConstants.PROJECT_ID, projectId);
    setIfValueNotNull(configuration, SpannerConstants.SERVICE_ACCOUNT_FILE_PATH, config.getServiceAccountFilePath());
    setIfValueNotNull(configuration, SpannerConstants.INSTANCE_ID, config.instance);
    setIfValueNotNull(configuration, SpannerConstants.DATABASE, config.database);
    setIfValueNotNull(configuration, SpannerConstants.QUERY, String.format("Select * from %s;", config.table));
  }

  private void setIfValueNotNull(Configuration configuration, String key, String value) {
    if (value != null) {
      configuration.set(key, value);
    }
  }

  /**
   * Serialize the object into bytes and encode the bytes into string using Base64 encoder.
   * @throws IOException
   */
  private String getSerializedObjectString(Object object) throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(object);
      objectOutputStream.flush();
      return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
    }
  }

  private PartitionOptions getPartitionOptions() {
    PartitionOptions.Builder builder = PartitionOptions.newBuilder();
    if (config.partitionSizeMB != null) {
      builder.setPartitionSizeBytes(config.partitionSizeMB * 1024 * 1024);
    }
    if (config.maxPartitions != null) {
      builder.setMaxPartitions(config.maxPartitions);
    }
    return builder.build();
  }

  private Schema getSchema() {
    String projectId = config.getProject();
    Spanner spanner;
    try {
      spanner = SpannerUtil.getSpannerService(config.getServiceAccountFilePath(), projectId);
    } catch (IOException e) {
      throw new InvalidStageException("Unable to get Spanner Client: " + e.getMessage(), e);
    }
    DatabaseClient databaseClient =
      spanner.getDatabaseClient(DatabaseId.of(projectId, config.instance, config.database));
    Statement getTableSchemaStatement = SCHEMA_STATEMENT_BUILDER.bind(TABLE_NAME).to(config.table).build();
    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(getTableSchemaStatement)) {
      List<Schema.Field> schemaFields = new ArrayList<>();
      while (resultSet.next()) {
        String columnName = resultSet.getString("column_name");
        String spannerType = resultSet.getString("spanner_type");
        String nullable = resultSet.getString("is_nullable");
        boolean isNullable = "YES".equals(nullable);
        Schema typeSchema = parseSchemaFromSpannerTypeString(columnName, spannerType);
        Schema fieldSchema = isNullable ? Schema.nullableOf(typeSchema) : typeSchema;
        schemaFields.add(Schema.Field.of(columnName, fieldSchema));
      }
      spanner.close();
      return Schema.recordOf("outputSchema", schemaFields);
    }
  }

  private Schema parseSchemaFromSpannerTypeString(String columnName,
                                                  String spannerType) {
    if (spannerType.startsWith("ARRAY")) {
      // Array string is of the format ARRAY<TYPE>, Array of array is not supported in spanner
      throw new InvalidStageException(String.format("'%s' is an array, which is not currently supported.", columnName));
    } else if (spannerType.startsWith("STRING")) {
      // STRING and BYTES also have size at the end in the format, example : STRING(1024)
      return Schema.of(Schema.Type.STRING);
    } else if (spannerType.startsWith("BYTES")) {
      return Schema.of(Schema.Type.BYTES);
    } else {
      switch (Type.Code.valueOf(spannerType)) {
        case BOOL:
          return Schema.of(Schema.Type.BOOLEAN);
        case INT64:
          return Schema.of(Schema.Type.LONG);
        case FLOAT64:
          return Schema.of(Schema.Type.DOUBLE);
        case DATE:
          return Schema.of(Schema.LogicalType.DATE);
        case TIMESTAMP:
          return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        // todo CDAP-14233 - add support for array
        default:
          throw new InvalidStageException(String.format("'%s' is of unsupported type '%s'", columnName, spannerType));
      }
    }
  }
}
