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

package io.cdap.plugin.gcp.bigtable.source;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.gcp.bigtable.common.HBaseColumn;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Batch Cloud Bigtable Source Plugin reads the data from Google Cloud Bigtable.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(BigtableSource.NAME)
@Description("This source reads data from Google Cloud Bigtable. " +
  "Cloud Bigtable is Google's NoSQL Big Data database service.")
public final class BigtableSource extends BatchSource<ImmutableBytesWritable, Result, StructuredRecord> {
  public static final String NAME = "Bigtable";
  private static final Logger LOG = LoggerFactory.getLogger(BigtableSource.class);
  private final BigtableSourceConfig config;
  private HBaseResultToRecordTransformer resultToRecordTransformer;

  public BigtableSource(BigtableSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    config.validate();
    Schema configuredSchema = config.getSchema();
    if (configuredSchema != null && config.connectionParamsConfigured()) {
      Configuration conf = getConfiguration();
      validateOutputSchema(conf);
    }

    configurer.getStageConfigurer().setOutputSchema(configuredSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    config.validate();
    Configuration conf = getConfiguration();
    validateOutputSchema(conf);

    // Both emitLineage and setOutputFormat internally try to create an external dataset if it does not already exists.
    // We call emitLineage before since it creates the dataset with schema.
    emitLineage(context);
    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(TableInputFormat.class, conf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    resultToRecordTransformer =
      new HBaseResultToRecordTransformer(context.getOutputSchema(), config.keyAlias, config.getColumnMappings());
  }

  /**
   * Converts <code>JsonObject</code> to <code>StructuredRecord</code> for every record
   * retrieved from the Bigtable.
   *
   * @param input   input record
   * @param emitter emitting the transformed record into downstream nodes.
   */
  @Override
  public void transform(KeyValue<ImmutableBytesWritable, Result> input, Emitter<StructuredRecord> emitter) {
    try {
      StructuredRecord record = resultToRecordTransformer.transform(input.getValue());
      emitter.emit(record);
    } catch (Exception e) {
      switch (config.getErrorHandling()) {
        case SKIP:
          LOG.warn("Failed to process message, skipping it", e);
          break;
        case FAIL_PIPELINE:
          throw new RuntimeException("Failed to process message", e);
        default:
          // this should never happen because it is validated at configure and prepare time
          throw new IllegalStateException(String.format("Unknown error handling strategy '%s'",
                                                        config.getErrorHandling()));
      }
    }
  }

  private Configuration getConfiguration() {
    try {
      Configuration conf = new Configuration();
      BigtableConfiguration.configure(conf, config.getProject(), config.instance);
      conf.setBoolean(TableInputFormat.SHUFFLE_MAPS, true);
      conf.set(TableInputFormat.INPUT_TABLE, config.table);
      config.getBigtableOptions().forEach(conf::set);
      Scan scan = getConfiguredScanForJob();
      conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
      return conf;
    } catch (IOException e) {
      throw new InvalidStageException("Failed to prepare configuration for job", e);
    }
  }

  private void validateOutputSchema(Configuration configuration) {
    TableName tableName = TableName.valueOf(config.table);
    try (Connection connection = BigtableConfiguration.connect(configuration);
         Table table = connection.getTable(tableName)) {
      Set<String> existingFamilies = table.getTableDescriptor()
        .getFamiliesKeys()
        .stream()
        .map(Bytes::toString)
        .collect(Collectors.toSet());
      for (HBaseColumn hBaseColumn : config.getRequestedColumns()) {
        if (!existingFamilies.contains(hBaseColumn.getFamily())) {
          throw new InvalidConfigPropertyException(
            String.format("Column family '%s' does not exist", hBaseColumn.getFamily()),
            BigtableSourceConfig.SCHEMA
          );
        }
      }
    } catch (IOException e) {
      throw new InvalidStageException("Failed to connect to Bigtable", e);
    }
  }

  private void emitLineage(BatchSourceContext context) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());

    List<Schema.Field> fields = Objects.requireNonNull(config.getSchema()).getFields();
    if (fields != null) {
      List<String> fieldNames = fields.stream()
        .map(Schema.Field::getName)
        .collect(Collectors.toList());
      String operationDescription = String.format("Read from Bigtable. Project: '%s', Instance: '%s'. Table: '%s'",
                                                  config.getProject(), config.instance, config.table);
      lineageRecorder.recordRead("Read", operationDescription, fieldNames);
    }
  }

  private Scan getConfiguredScanForJob() throws IOException {
    Scan s = new Scan();
    if (config.scanTimeRangeStart != null || config.scanTimeRangeStop != null) {
      long scanTimeRangeStart = ObjectUtils.defaultIfNull(config.scanTimeRangeStart, 0L);
      long scanTimeRangeStop = ObjectUtils.defaultIfNull(config.scanTimeRangeStop, Long.MAX_VALUE);
      s.setTimeRange(scanTimeRangeStart, scanTimeRangeStop);
    }
    s.setCacheBlocks(false);
    if (config.scanRowStart != null) {
      s.withStartRow(Bytes.toBytes(config.scanRowStart));
    }
    if (config.scanRowStop != null) {
      s.withStopRow(Bytes.toBytes(config.scanRowStop));
    }
    for (HBaseColumn hBaseColumn : config.getRequestedColumns()) {
      s.addColumn(Bytes.toBytes(hBaseColumn.getFamily()), Bytes.toBytes(hBaseColumn.getQualifier()));
    }
    return s;
  }
}
