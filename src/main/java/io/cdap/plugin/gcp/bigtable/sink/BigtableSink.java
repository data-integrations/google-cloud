/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigtable.sink;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.gcp.common.SourceOutputFormatProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link BatchSink} that writes data to Cloud Bigtable.
 * This plugin takes a {@link StructuredRecord} in, converts it to {@link Put} mutation, and writes it to the
 * Cloud Bigtable instance.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(BigtableSink.NAME)
@Description("This sink writes data to Google Cloud Bigtable. " +
  "Cloud Bigtable is Google's NoSQL Big Data database service.")
public final class BigtableSink extends BatchSink<StructuredRecord, ImmutableBytesWritable, Mutation> {
  public static final String NAME = "Bigtable";
  private static final Logger LOG = LoggerFactory.getLogger(BigtableSink.class);

  private static final Set<Schema.Type> SUPPORTED_FIELD_TYPES = ImmutableSet.of(
    Schema.Type.BOOLEAN,
    Schema.Type.INT,
    Schema.Type.LONG,
    Schema.Type.FLOAT,
    Schema.Type.DOUBLE,
    Schema.Type.BYTES,
    Schema.Type.STRING
  );

  private final BigtableSinkConfig config;
  private RecordToHBaseMutationTransformer transformer;

  public BigtableSink(BigtableSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    config.validate();
    Schema inputSchema = configurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      validateInputSchema(inputSchema);
    }
    if (config.connectionParamsConfigured()) {
      Configuration conf = getConfiguration();
      try (Connection connection = BigtableConfiguration.connect(conf);
           Admin admin = connection.getAdmin()) {
        TableName tableName = TableName.valueOf(config.table);
        if (admin.tableExists(tableName)) {
          validateExistingTable(connection, tableName);
        }
      } catch (IOException e) {
        throw new InvalidStageException("Failed to connect to Bigtable", e);
      }
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    config.validate();
    Configuration conf = getConfiguration();
    try (Connection connection = BigtableConfiguration.connect(conf);
         Admin admin = connection.getAdmin()) {
      TableName tableName = TableName.valueOf(config.table);
      if (admin.tableExists(tableName)) {
        validateExistingTable(connection, tableName);
      } else {
        createTable(connection, tableName);
      }
    } catch (IOException e) {
      throw new InvalidStageException("Failed to connect to Bigtable", e);
    }

    // Both emitLineage and setOutputFormat internally try to create an external dataset if it does not already exists.
    // We call emitLineage before since it creates the dataset with schema.
    emitLineage(context);
    context.addOutput(Output.of(config.referenceName, new SourceOutputFormatProvider(TableOutputFormat.class, conf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    transformer = new RecordToHBaseMutationTransformer(config.keyAlias, config.columnFamily);
  }

  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<ImmutableBytesWritable, Mutation>> emitter) {
    Mutation mutation = transformer.transform(record);
    emitter.emit(new KeyValue<>(null, mutation));
  }

  private Configuration getConfiguration() {
    Configuration conf = HBaseConfiguration.create();
    BigtableConfiguration.configure(conf, config.getProject(), config.instance);
    conf.set(BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY, "60000");
    conf.set(TableOutputFormat.OUTPUT_TABLE, config.table);
    return conf;
  }

  private void validateInputSchema(Schema inputSchema) {
    if (inputSchema.getField(config.keyAlias) == null) {
      throw new InvalidConfigPropertyException(
        String.format("Field '%s' declared as key alias does not exist in input schema", config.keyAlias),
        BigtableSinkConfig.KEY_ALIAS
      );
    }
    List<Schema.Field> fields = inputSchema.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new InvalidStageException("Input schema should contain fields");
    }
    for (Schema.Field field : fields) {
      Schema.Type fieldType = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable().getType() : field.getSchema().getType();
      if (!SUPPORTED_FIELD_TYPES.contains(field.getSchema().getType())) {
        String supportedTypes = SUPPORTED_FIELD_TYPES.stream()
          .map(Enum::name)
          .map(String::toLowerCase)
          .collect(Collectors.joining(", "));
        String errorMessage = String.format("Field '%s' is of unsupported type '%s'. Supported types are: %s.",
                                            field.getName(), fieldType, supportedTypes);
        throw new InvalidStageException(errorMessage);
      }
    }
  }

  private void createTable(Connection connection, TableName tableName) {
    try (Admin admin = connection.getAdmin()) {
      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
      tableDescriptor.addFamily(new HColumnDescriptor(config.columnFamily));
      admin.createTable(tableDescriptor);
    } catch (IOException e) {
      throw new InvalidStageException(String.format("Failed to create table '%s' in Bigtable", tableName), e);
    }
  }

  private void validateExistingTable(Connection connection, TableName tableName) {
    try (Table table = connection.getTable(tableName)) {
      if (!table.getTableDescriptor().hasFamily(Bytes.toBytes(config.columnFamily))) {
        throw new InvalidConfigPropertyException(
          String.format("Column family '%s' does not exist", config.columnFamily),
          BigtableSinkConfig.COLUMN_FAMILY
        );
      }
    } catch (IOException e) {
      throw new InvalidStageException("Failed to connect to Bigtable", e);
    }
  }

  private void emitLineage(BatchSinkContext context) {
    Schema inputSchema = context.getInputSchema();
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(inputSchema);

    if (inputSchema != null) {
      List<Schema.Field> fields = inputSchema.getFields();
      if (fields != null) {
        List<String> fieldNames = fields.stream()
          .map(Schema.Field::getName)
          .collect(Collectors.toList());
        String operationDescription = String.format("Wrote to Bigtable. Project: '%s', Instance: '%s'. Table: '%s'",
                                                    config.getProject(), config.instance, config.table);
        lineageRecorder.recordWrite("Write", operationDescription, fieldNames);
      }
    }
  }
}
