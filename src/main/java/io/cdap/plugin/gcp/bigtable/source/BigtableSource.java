/*
 * Copyright © 2019-2020 Cask Data, Inc.
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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.gcp.bigtable.common.HBaseColumn;
import io.cdap.plugin.gcp.common.ConfigUtil;
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
import java.util.Map;
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
    StageConfigurer stageConfigurer = configurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
    Schema configuredSchema = config.getSchema(collector);
    if (configuredSchema != null && config.connectionParamsConfigured()) {
      try {
        Configuration conf = getConfiguration(collector);
        validateOutputSchema(conf, collector);
      } catch (IOException e) {
        // Don't fail deployment on connection failure
        LOG.warn("Failed to validate output schema", e);
      }
    }

    stageConfigurer.setOutputSchema(configuredSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    Schema configuredSchema = config.getSchema(collector);
    Configuration conf = null;
    try {
      conf = getConfiguration(collector);

    } catch (IOException e) {
      collector.addFailure(
        String.format("Failed to prepare configuration for job : %s", e.getMessage()), null)
        .withConfigProperty(BigtableSourceConfig.BIGTABLE_OPTIONS)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }
    try {
      validateOutputSchema(conf, collector);
    } catch (IOException e) {
      collector.addFailure(String.format("Failed to connect to Bigtable : %s", e.getMessage()), null)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }

    // Both emitLineage and setOutputFormat internally try to create an external dataset if it does not already exists.
    // We call emitLineage before since it creates the dataset with schema.
    emitLineage(context, configuredSchema);
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

  private Configuration getConfiguration(FailureCollector collector) throws IOException {
    Configuration conf = new Configuration();
    BigtableConfiguration.configure(conf, config.getProject(), config.instance);
    conf.setBoolean(TableInputFormat.SHUFFLE_MAPS, true);
    conf.set(TableInputFormat.INPUT_TABLE, config.table);
    config.getBigtableOptions().forEach(conf::set);
    Scan scan = getConfiguredScanForJob(collector);
    conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
    return conf;
  }

  private void validateOutputSchema(Configuration configuration, FailureCollector collector) throws IOException {
    TableName tableName = TableName.valueOf(config.table);
    try (Connection connection = BigtableConfiguration.connect(configuration);
         Table table = connection.getTable(tableName)) {
      Set<String> existingFamilies = table.getTableDescriptor()
        .getFamiliesKeys()
        .stream()
        .map(Bytes::toString)
        .collect(Collectors.toSet());
      for (HBaseColumn hBaseColumn : config.getRequestedColumns(collector)) {
        if (!existingFamilies.contains(hBaseColumn.getFamily())) {
          Map<String, String> columnMappings = config.getColumnMappings();
          String key = hBaseColumn.getQualifiedName();
          collector.addFailure(String.format("Column family '%s' does not exist.", hBaseColumn.getFamily()),
                               "Specify correct column family.")
            .withConfigElement(BigtableSourceConfig.COLUMN_MAPPINGS,
                               ConfigUtil.getKVPair(key, columnMappings.get(key), "="));
        }
      }
    }
  }

  private void emitLineage(BatchSourceContext context, Schema schema) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(schema);

    List<Schema.Field> fields = Objects.requireNonNull(config.getSchema(context.getFailureCollector())).getFields();
    if (fields != null) {
      List<String> fieldNames = fields.stream()
        .map(Schema.Field::getName)
        .collect(Collectors.toList());
      String operationDescription = String.format("Read from Bigtable. Project: '%s', Instance: '%s'. Table: '%s'",
                                                  config.getProject(), config.instance, config.table);
      lineageRecorder.recordRead("Read", operationDescription, fieldNames);
    }
  }

  private Scan getConfiguredScanForJob(FailureCollector collector) {
    Scan s = new Scan();
    try {
      if (config.scanTimeRangeStart != null || config.scanTimeRangeStop != null) {
        long scanTimeRangeStart = ObjectUtils.defaultIfNull(config.scanTimeRangeStart, 0L);
        long scanTimeRangeStop = ObjectUtils.defaultIfNull(config.scanTimeRangeStop, Long.MAX_VALUE);
        s.setTimeRange(scanTimeRangeStart, scanTimeRangeStop);
      }
    } catch (IOException e) {
      collector.addFailure(String.format("Unable to set time range configuration : %s", e.getMessage()), null)
        .withConfigProperty(BigtableSourceConfig.SCAN_TIME_RANGE_START)
        .withConfigProperty(BigtableSourceConfig.SCAN_TIME_RANGE_STOP).withStacktrace(e.getStackTrace());
    }
    s.setCacheBlocks(false);
    if (config.scanRowStart != null) {
      s.withStartRow(Bytes.toBytes(config.scanRowStart));
    }
    if (config.scanRowStop != null) {
      s.withStopRow(Bytes.toBytes(config.scanRowStop));
    }
    for (HBaseColumn hBaseColumn : config.getRequestedColumns(collector)) {
      s.addColumn(Bytes.toBytes(hBaseColumn.getFamily()), Bytes.toBytes(hBaseColumn.getQualifier()));
    }
    return s;
  }
}
