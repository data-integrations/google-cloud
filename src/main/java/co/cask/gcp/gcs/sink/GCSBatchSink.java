/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.gcp.gcs.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.gcp.common.GCPReferenceSinkConfig;
import co.cask.gcp.format.FileFormat;
import co.cask.gcp.format.output.FileOutputFormatter;
import co.cask.gcp.gcs.GCSConfigHelper;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import com.google.common.base.Strings;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Writes data to files on Google Cloud Storage.
 *
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCS")
@Description("Writes records to one or more files in a directory on Google Cloud Storage.")
public class GCSBatchSink<KEY_OUT, VAL_OUT> extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {
  private final GCSBatchSinkConfig config;
  private FileOutputFormatter<KEY_OUT, VAL_OUT> outputFormatter;

  public GCSBatchSink(GCSBatchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public final void prepareRun(BatchSinkContext context) {
    config.validate();

    // set common properties
    Map<String, String> outputConfig = new HashMap<>();
    outputConfig.put(FileOutputFormat.OUTDIR, config.getOutputDir(context.getLogicalStartTime()));
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      outputConfig.put("google.cloud.auth.service.account.json.keyfile", serviceAccountFilePath);
    }
    outputConfig.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    outputConfig.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    String projectId = config.getProject();
    outputConfig.put("fs.gs.project.id", projectId);
    outputConfig.put("fs.gs.system.bucket", GCSConfigHelper.getBucket(config.path));
    outputConfig.put("fs.gs.impl.disable.cache", "true");
    
    // set format specific properties.
    outputFormatter = config.getFileFormat().getFileOutputFormatter(config.getProperties().getProperties(),
                                                                    config.getSchema());
    if (outputFormatter == null) {
      // should never happen, as validation should enforce the allowed formats
      throw new IllegalArgumentException(String.format("Format '%s' cannot be used to write data.", config.format));
    }

    outputConfig.putAll(outputFormatter.getFormatConfig());

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(config.getSchema());

    context.addOutput(Output.of(config.referenceName,
                                new SinkOutputFormatProvider(outputFormatter.getFormatClassName(), outputConfig)));
    // record field level lineage information
    Schema schema = config.getSchema();
    if (schema != null && schema.getFields() != null && !schema.getFields().isEmpty()) {
      lineageRecorder.recordWrite("Write", "Wrote to Google Cloud Storage.",
                                  schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    outputFormatter = config.getFileFormat().getFileOutputFormatter(config.getProperties().getProperties(),
                                                                    config.getSchema());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<KEY_OUT, VAL_OUT>> emitter) throws Exception {
    emitter.emit(outputFormatter.transform(input));
  }

  /**
   * Sink configuration.
   */
  @SuppressWarnings("unused")
  public static class GCSBatchSinkConfig extends GCPReferenceSinkConfig {
    @Name("path")
    @Description("The path to write to. For example, gs://<bucket>/path/to/directory")
    @Macro
    private String path;

    @Description("The time format for the output directory that will be appended to the path. " +
      "For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. " +
      "If not specified, nothing will be appended to the path.")
    @Nullable
    @Macro
    private String suffix;

    @Description("The format to write in. The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', "
      + "or 'delimited'.")
    @Macro
    private String format;

    @Description("The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the format "
      + "is anything other than 'delimited'.")
    @Macro
    @Nullable
    private String delimiter;

    @Description("The schema of the data to write. The 'avro' and 'parquet' formats require a schema but other "
      + "formats do not.")
    @Macro
    @Nullable
    private String schema;

    private void validate() {
      GCSConfigHelper.getPath(path);
      if (suffix != null && !containsMacro("suffix")) {
        new SimpleDateFormat(suffix);
      }
      if (!containsMacro("format")) {
        getFileFormat();
      }
      getSchema();
    }

    @Nullable
    private Schema getSchema() {
      if (containsMacro("schema") || schema == null) {
        return null;
      }
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
      }
    }

    /**
     * Logically equivalent to valueOf except it throws an exception with a message that indicates what the valid
     * enum values are.
     */
    private FileFormat getFileFormat() {
      try {
        return FileFormat.valueOf(format.toUpperCase());
      } catch (IllegalArgumentException e) {
        String values = Arrays.stream(FileFormat.values())
          .filter(f -> f != FileFormat.BLOB)
          .map(f -> f.name().toLowerCase())
          .collect(Collectors.joining(", "));
        throw new IllegalArgumentException(String.format("Invalid format '%s'. The value must be one of %s",
                                                         format, values));
      }
    }

    private String getOutputDir(long logicalStartTime) {
      String timeSuffix = !Strings.isNullOrEmpty(suffix) ? new SimpleDateFormat(suffix).format(logicalStartTime) : "";
      return String.format("%s/%s", GCSConfigHelper.getPath(path), timeSuffix);
    }
  }
}
