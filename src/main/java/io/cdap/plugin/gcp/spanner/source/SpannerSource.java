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

package io.cdap.plugin.gcp.spanner.source;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.Schemas;
import io.cdap.plugin.gcp.spanner.SpannerConstants;
import io.cdap.plugin.gcp.spanner.common.SpannerUtil;
import io.cdap.plugin.gcp.spanner.connector.SpannerConnector;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Cloud Spanner batch source.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SpannerSource.NAME)
@Description("Batch source to read from Cloud Spanner. Cloud Spanner is a fully managed, mission-critical, " +
  "relational database service that offers transactional consistency at global scale, schemas, " +
  "SQL (ANSI 2011 with extensions), and automatic, synchronous replication for high availability.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = SpannerConnector.NAME)})
public class SpannerSource extends BatchSource<NullWritable, ResultSet, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerSource.class);
  private static final String TABLE_NAME = "TableName";
  // listing table's schema documented at https://cloud.google.com/spanner/docs/information-schema
  private static final Statement.Builder SCHEMA_STATEMENT_BUILDER = Statement.newBuilder(
    String.format("SELECT  t.column_name,t.spanner_type, t.is_nullable FROM information_schema.columns AS t WHERE " +
                    "  t.table_catalog = ''  AND  t.table_schema = '' AND t.table_name = @%s", TABLE_NAME));
  private static final String LIMIT = "limit";

  public static final String NAME = "Spanner";
  private final SpannerSourceConfig config;
  private Schema schema;
  private ResultSetToRecordTransformer transformer;

  public SpannerSource(SpannerSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
    Schema configuredSchema = config.getSchema(collector);

    if (!config.canConnect() || config.tryGetProject() == null
      || (config.isServiceAccountFilePath() && config.autoServiceAccountUnavailable())) {
      stageConfigurer.setOutputSchema(configuredSchema);
      return;
    }

    try {
      Schema schema = getSchema(collector);
      if (configuredSchema == null) {
        stageConfigurer.setOutputSchema(schema);
        return;
      }

      Schemas.validateFieldsMatch(schema, configuredSchema, collector);
      stageConfigurer.setOutputSchema(configuredSchema);
    } catch (SpannerException e) {
      // this is because spanner exception error message is not very user friendly. It contains class names and new
      // lines in the error message.
      collector.addFailure("Unable to connect to spanner instance.",
                           "Verify spanner configurations such as instance, database, table, project, etc.")
        .withStacktrace(e.getStackTrace());
    }
  }

  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) throws Exception {
    FailureCollector collector = batchSourceContext.getFailureCollector();
    config.validate(collector);
    Schema actualSchema = getSchema(collector);
    Schema configuredSchema = config.getSchema(collector);
    if (configuredSchema != null) {
      Schemas.validateFieldsMatch(actualSchema, configuredSchema, collector);
    }
    // throw a validation exception if any failures were added to the collector.
    collector.getOrThrowException();
    String projectId = config.getProject();
    Configuration configuration = new Configuration();
    initializeConfig(configuration, projectId);
    String location;

    // initialize spanner
    String instanceConfigId;
    try (Spanner spanner = SpannerUtil.getSpannerService(config.getServiceAccount(), config.isServiceAccountFilePath(),
                                                         projectId)) {
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
      String importQuery = Strings.isNullOrEmpty(config.importQuery) ?
        String.format("Select * from %s;", config.table) : config.importQuery;
      List<Partition> partitions =
        new ArrayList<>(
          batchReadOnlyTransaction.partitionQuery(getPartitionOptions(),
                                                  Statement.of(importQuery)));

      // serialize batch transaction-id and partitions
      configuration.set(SpannerConstants.SPANNER_BATCH_TRANSACTION_ID, getSerializedObjectString(batchTransactionId));
      configuration.set(SpannerConstants.PARTITIONS_LIST, getSerializedObjectString(partitions));
      // get location for lineage data (location info resides in instance config for spanner)
      Instance spannerInstance = spanner.getInstanceAdminClient().getInstance(config.instance);
      instanceConfigId = spannerInstance.getInstanceConfigId().getInstanceConfig();
      InstanceConfig instanceConfig = spanner.getInstanceAdminClient()
          .getInstanceConfig(instanceConfigId);
      location = instanceConfig.getReplicas().get(0).getLocation();
    }

    String fqn = SpannerUtil.getFQN(config.getConnection().getProject(), instanceConfigId,
        config.getInstance(), config.getDatabase(), config.getTable());
    String referenceName =
        Strings.isNullOrEmpty(config.getReferenceName()) ? ReferenceNames.normalizeFqn(fqn)
            : config.getReferenceName();
    Asset asset = Asset.builder(referenceName).setFqn(fqn).setLocation(location).build();
    LineageRecorder lineageRecorder = new LineageRecorder(batchSourceContext, asset);
    lineageRecorder.createExternalDataset(configuredSchema);

    // set input format and pass configuration
    batchSourceContext.setInput(Input.of(referenceName,
                                         new SourceInputFormatProvider(SpannerInputFormat.class, configuration)));
    schema = batchSourceContext.getOutputSchema();
    if (schema != null) {
      if (schema.getFields() != null) {
        lineageRecorder.recordRead("Read", "Read from Spanner table.",
                                   schema.getFields().stream().map(Schema.Field::getName)
                                     .collect(Collectors.toList()));
      }
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    // this will be null if schema is macro enabled or not set at configure time
    Schema configuredSchema = context.getOutputSchema();
    schema = configuredSchema == null ? getSchema(context.getFailureCollector()) : configuredSchema;
    transformer = new ResultSetToRecordTransformer(schema);
  }

  @Override
  public void transform(KeyValue<NullWritable, ResultSet> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(transformer.transform(input.getValue()));
  }

  private void initializeConfig(Configuration configuration, String projectId) {
    setIfValueNotNull(configuration, SpannerConstants.PROJECT_ID, projectId);
    setIfValueNotNull(configuration, SpannerConstants.SERVICE_ACCOUNT_TYPE, config.isServiceAccountFilePath() ?
      SpannerConstants.SERVICE_ACCOUNT_TYPE_FILE_PATH : SpannerConstants.SERVICE_ACCOUNT_TYPE_JSON);
    setIfValueNotNull(configuration, SpannerConstants.SERVICE_ACCOUNT, config.getServiceAccount());
    setIfValueNotNull(configuration, SpannerConstants.INSTANCE_ID, config.instance);
    setIfValueNotNull(configuration, SpannerConstants.DATABASE, config.database);
    setIfValueNotNull(configuration, SpannerConstants.QUERY, Strings.isNullOrEmpty(config.importQuery) ?
      String.format("Select * from %s;", config.table) : config.importQuery);
  }

  private void setIfValueNotNull(Configuration configuration, String key, String value) {
    if (value != null) {
      configuration.set(key, value);
    }
  }

  /**
   * Serialize the object into bytes and encode the bytes into string using Base64 encoder.
   *
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

  private Schema getSchema(FailureCollector collector) {
    String projectId = config.getProject();

    try (Spanner spanner = SpannerUtil.getSpannerService(config.getServiceAccount(), config.isServiceAccountFilePath(),
                                                         projectId)) {
      if (Strings.isNullOrEmpty(config.importQuery)) {
        return SpannerUtil.getTableSchema(spanner, projectId, config.instance, config.database,
                                          config.table, collector);
      } else {
        DatabaseClient databaseClient =
          spanner.getDatabaseClient(DatabaseId.of(projectId, config.instance, config.database));
        final Map<String, Boolean> nullableFields = getFieldsNullability(databaseClient);
        Statement importQueryStatement = getStatementForOneRow(config.importQuery);
        List<Schema.Field> schemaFields = new ArrayList<>();
        try (ResultSet resultSet = databaseClient.singleUse().executeQuery(importQueryStatement)) {
          while (resultSet.next()) {
            final List<Type.StructField> structFields = resultSet.getCurrentRowAsStruct().getType().getStructFields();
            for (Type.StructField structField : structFields) {
              final Type fieldSpannerType = structField.getType();
              final String columnName = structField.getName();
              // there are cases when column name is not in metadata table such as "Select FirstName as name",
              // so fallback is nullable
              final boolean isNullable = nullableFields.getOrDefault(columnName, true);
              final Schema typeSchema = parseSchemaFromSpannerType(fieldSpannerType, columnName, collector);
              if (typeSchema == null) {
                // this means there were failures added to failure collector. Continue to collect more failures
                continue;
              }
              Schema fieldSchema = isNullable ? Schema.nullableOf(typeSchema) : typeSchema;
              schemaFields.add(Schema.Field.of(columnName, fieldSchema));
            }
          }
          if (schemaFields.isEmpty() && !collector.getValidationFailures().isEmpty()) {
            collector.getOrThrowException();
          }
          return Schema.recordOf("outputSchema", schemaFields);
        }
      }
    } catch (IOException e) {
      collector.addFailure("Unable to get Spanner Client: " + e.getMessage(), null)
        .withConfigProperty(config.isServiceAccountFilePath() ?
                              GCPConfig.NAME_SERVICE_ACCOUNT_FILE_PATH : GCPConfig.NAME_SERVICE_ACCOUNT_JSON);
      // if there was an error that was added, it will throw an exception.
      throw collector.getOrThrowException();
    }

  }

  private Statement getStatementForOneRow(String importQuery) {
    String query;
    // Matches any String containing the word 'limit' followed by a number
    // ex: SELECT NAME FROM TABLE LIMIT 15
    String regex = "^(?:[^;']|(?:'[^']+'))+ LIMIT +\\d+(.*)";
    Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
    if (pattern.matcher(importQuery).matches()) {
      int index = StringUtils.lastIndexOf(importQuery, LIMIT);
      String substringToReplace = importQuery.substring(index);
      query = importQuery.replace(substringToReplace, "limit 1");
    } else {
      query = String.format("%s limit 1", importQuery);
    }
    return Statement.newBuilder(query).build();
  }


  @Nullable
  Schema parseSchemaFromSpannerType(Type spannerType, String columnName, FailureCollector collector) {
    final Type.Code code = spannerType.getCode();

    if (code == Type.Code.ARRAY) {
      final Type arrayElementType = spannerType.getArrayElementType();
      final Type.Code arrayElementTypeCode = arrayElementType.getCode();
      switch (arrayElementTypeCode) {
        case BOOL:
          return Schema.arrayOf(Schema.of(Schema.Type.BOOLEAN));
        case INT64:
          return Schema.arrayOf(Schema.of(Schema.Type.LONG));
        case FLOAT64:
          return Schema.arrayOf(Schema.of(Schema.Type.DOUBLE));
        case STRING:
          return Schema.arrayOf(Schema.of(Schema.Type.STRING));
        case BYTES:
          return Schema.arrayOf(Schema.of(Schema.Type.BYTES));
        case TIMESTAMP:
          return Schema.arrayOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
        case DATE:
          return Schema.arrayOf(Schema.of(Schema.LogicalType.DATE));
        default:
          collector.addFailure(String.format("Column '%s' has unsupported type '%s'.", columnName, spannerType), null);
          return null;
      }
    } else {
      switch (code) {
        case BOOL:
          return Schema.of(Schema.Type.BOOLEAN);
        case INT64:
          return Schema.of(Schema.Type.LONG);
        case FLOAT64:
          return Schema.of(Schema.Type.DOUBLE);
        case STRING:
          return Schema.of(Schema.Type.STRING);
        case BYTES:
          return Schema.of(Schema.Type.BYTES);
        case TIMESTAMP:
          return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        case DATE:
          return Schema.of(Schema.LogicalType.DATE);
        default:
          collector.addFailure(String.format("Column '%s' has unsupported type '%s'.", columnName, spannerType), null);
          return null;
      }
    }
  }

  /** Get from table metadata nullability for each field
   * @param databaseClient Database Client
   * @return Map where key is field name and value is nullability true or false
   */
  private Map<String, Boolean> getFieldsNullability(DatabaseClient databaseClient) {
    Statement tableMetadataStatement = SCHEMA_STATEMENT_BUILDER.bind(TABLE_NAME).to(config.table).build();
    Map<String, Boolean> nullableState = new HashMap<>();
    ResultSet resultSet = databaseClient.singleUse().executeQuery(tableMetadataStatement);
    while (resultSet.next()) {
      String columnName = resultSet.getString("column_name");
      String nullable = resultSet.getString("is_nullable");
      nullableState.put(columnName, "YES".equals(nullable));
    }
    return nullableState;
  }
}
