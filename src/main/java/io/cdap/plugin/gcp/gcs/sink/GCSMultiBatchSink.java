/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs.sink;

import com.google.auth.Credentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * {@link GCSMultiBatchSink} that stores the data of the latest run of an adapter in GCS.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCSMultiFiles")
@Description("Writes records to one or more Avro, ORC, Parquet or Delimited format files in a directory " +
        "on Google Cloud Storage.")
public class GCSMultiBatchSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  private static final String TABLE_PREFIX = "multisink.";
  private static final String FORMAT_PLUGIN_ID = "format";
  private static final String SCHEMA_MACRO = "__provided_schema__";

  private final GCSMultiBatchSinkConfig config;

  public GCSMultiBatchSink(GCSMultiBatchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    FileFormat format = config.getFormat();
    // add schema as a macro since we don't know it until runtime
    PluginProperties formatProperties = PluginProperties.builder()
      .addAll(config.getProperties().getProperties())
      .add("schema", String.format("${%s}", SCHEMA_MACRO)).build();

    OutputFormatProvider outputFormatProvider =
      pipelineConfigurer.usePlugin(ValidatingOutputFormat.PLUGIN_TYPE, format.name().toLowerCase(),
                                   FORMAT_PLUGIN_ID, formatProperties);
    if (outputFormatProvider == null) {
      collector.addFailure(
        String.format("Could not find the '%s' output format plugin.", format.name().toLowerCase()), null)
        .withPluginNotFound(FORMAT_PLUGIN_ID, format.name().toLowerCase(), ValidatingOutputFormat.PLUGIN_TYPE);
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws IOException, InstantiationException {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Map<String, String> baseProperties = GCPUtils.getFileSystemProperties(config, config.getPath(), new HashMap<>());
    Map<String, String> argumentCopy = new HashMap<>(context.getArguments().asMap());

    String cmekKey = context.getArguments().get(GCPUtils.CMEK_KEY);
    Boolean isServiceAccountFilePath = config.isServiceAccountFilePath();
    if (isServiceAccountFilePath == null) {
      context.getFailureCollector().addFailure("Service account type is undefined.",
                                               "Must be `filePath` or `JSON`");
      context.getFailureCollector().getOrThrowException();
      return;
    }
    Credentials credentials = config.getServiceAccount() == null ?
      null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(), isServiceAccountFilePath);
    Storage storage = GCPUtils.getStorage(config.getProject(), credentials);
    try {
      if (storage.get(config.getBucket()) == null) {
        GCPUtils.createBucket(storage, config.getBucket(), config.getLocation(), cmekKey);
      }
    } catch (StorageException e) {
      // Add more descriptive error message
      throw new RuntimeException(
        String.format("Unable to access or create bucket %s. ", config.getBucket())
          + "Ensure you entered the correct bucket path and have permissions for it.", e);
    }

    for (Map.Entry<String, String> argument : argumentCopy.entrySet()) {
      String key = argument.getKey();
      if (!key.startsWith(TABLE_PREFIX)) {
        continue;
      }
      String name = key.substring(TABLE_PREFIX.length());
      Schema schema = Schema.parseJson(argument.getValue());
      // TODO: (CDAP-14600) pass in schema as an argument instead of using macros and setting arguments
      // add better platform support to allow passing in arguments when instantiating a plugin
      context.getArguments().set(SCHEMA_MACRO, schema.toString());
      ValidatingOutputFormat validatingOutputFormat = context.newPluginInstance(FORMAT_PLUGIN_ID);

      Map<String, String> outputProperties = new HashMap<>(baseProperties);
      outputProperties.putAll(validatingOutputFormat.getOutputFormatConfiguration());
      outputProperties.putAll(RecordFilterOutputFormat.configure(validatingOutputFormat.getOutputFormatClassName(),
                                                                 config.splitField, name, schema));
      outputProperties.put(FileOutputFormat.OUTDIR, config.getOutputDir(context.getLogicalStartTime(), name));
      outputProperties.put("mapreduce.fileoutputcommitter.algorithm.version", "2");

      outputProperties.put(GCSBatchSink.CONTENT_TYPE, config.getContentType());
      context.addOutput(Output.of(
        config.getReferenceName() + "_" + name,
        new SinkOutputFormatProvider(RecordFilterOutputFormat.class.getName(), outputProperties)));
    }
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
  }

  /**
   * Sink configuration.
   */
  public static class GCSMultiBatchSinkConfig extends GCSBatchSink.GCSBatchSinkConfig {

    @Description("The codec to use when writing data. " +
      "The 'avro' format supports 'snappy' and 'deflate'. The parquet format supports 'snappy' and 'gzip'. " +
      "Other formats do not support compression.")
    @Nullable
    private String compressionCodec;

    @Description("The name of the field that will be used to determine which directory to write to.")
    private String splitField = "tablename";

    protected String getOutputDir(long logicalStartTime, String context) {
      boolean suffixOk = !Strings.isNullOrEmpty(getSuffix());
      String timeSuffix = suffixOk ? new SimpleDateFormat(getSuffix()).format(logicalStartTime) : "";
      return String.format("%s/%s/%s", getPath(), context, timeSuffix);
    }
  }
}
