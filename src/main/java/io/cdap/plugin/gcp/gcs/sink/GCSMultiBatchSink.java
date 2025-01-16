/*
 * Copyright © 2015-2021 Cask Data, Inc.
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
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.InvalidPluginProperty;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProviderSpec;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSErrorDetailsProvider;
import io.cdap.plugin.gcp.gcs.connector.GCSConnector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


/**
 * {@link GCSMultiBatchSink} that stores the data of the latest run of an adapter in GCS.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(GCSMultiBatchSink.NAME)
@Description("Writes records to one or more Avro, ORC, Parquet or Delimited format files in a directory " +
  "on Google Cloud Storage.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = GCSConnector.NAME)})
public class GCSMultiBatchSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(GCSMultiBatchSink.class);
  public static final String NAME = "GCSMultiFiles";
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

    // add schema as a macro since we don't know it until runtime
    PluginProperties.Builder formatPropertiesBuilder = PluginProperties.builder()
      .addAll(config.getProperties().getProperties());

    if (!config.getAllowFlexibleSchema()) {
      formatPropertiesBuilder.add("schema", String.format("${%s}", SCHEMA_MACRO));
    }

    PluginProperties formatProperties = formatPropertiesBuilder.build();

    if (!this.config.containsMacro("format")) {
      String format = config.getFormatName();
      OutputFormatProvider outputFormatProvider =
              pipelineConfigurer.usePlugin(ValidatingOutputFormat.PLUGIN_TYPE, format, FORMAT_PLUGIN_ID,
                      formatProperties);
      if (outputFormatProvider == null) {
        collector.addFailure(
                String.format("Could not find the '%s' output format plugin.", format), null)
                .withPluginNotFound(FORMAT_PLUGIN_ID, format, ValidatingOutputFormat.PLUGIN_TYPE);
      }
      return;
    }
    //deploying all format plugins if its macro, so that required format plugin is available when macro is resolved
    for (FileFormat f: FileFormat.values()) {
      try {
        pipelineConfigurer.usePlugin(ValidatingOutputFormat.PLUGIN_TYPE, f.name().toLowerCase(),
                f.name().toLowerCase(), this.config.getRawProperties());
      } catch (InvalidPluginConfigException e) {
        LOG.warn("Failed to register format '{}', which means it cannot be used when the pipeline is run." +
                " Missing properties: {}, invalid properties: {}", new Object[]{f.name(),
                e.getMissingProperties(), e.getInvalidProperties().stream()
                .map(InvalidPluginProperty::getName).collect(Collectors.toList())});
      }
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector, context.getArguments().asMap());
    collector.getOrThrowException();

    Map<String, String> argumentCopy = new HashMap<>(context.getArguments().asMap());

    CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(config.cmekKey, context.getArguments().asMap(), collector);
    collector.getOrThrowException();

    Boolean isServiceAccountFilePath = config.connection.isServiceAccountFilePath();
    if (isServiceAccountFilePath == null) {
      collector.addFailure("Service account type is undefined.",
                                               "Must be `filePath` or `JSON`");
      collector.getOrThrowException();
    }

    Credentials credentials = null;
    try {
      credentials = config.connection.getServiceAccount() == null ?
        null : GCPUtils.loadServiceAccountCredentials(config.connection.getServiceAccount(),
        isServiceAccountFilePath);
    } catch (Exception e) {
      String errorReason = "Unable to load service account credentials.";
      collector.addFailure(String.format("%s %s", errorReason, e.getMessage()), null)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }

    String bucketName = config.getBucket(collector);
    Storage storage = GCPUtils.getStorage(config.connection.getProject(), credentials);
    try {
      if (storage.get(bucketName) == null) {
        GCPUtils.createBucket(storage, bucketName, config.getLocation(), cmekKeyName);
      }
    } catch (StorageException e) {
      String errorReasonFormat = "Error code: %s, Unable to read or access GCS bucket.";
      String correctiveAction = "Ensure you entered the correct bucket path and "
          + "have permissions for it.";
      String errorReason = String.format(errorReasonFormat, e.getCode());
      collector.addFailure(String.format("%s %s", errorReason, e.getMessage()), correctiveAction)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }

    // set error details provider
    context.setErrorDetailsProvider(
      new ErrorDetailsProviderSpec(GCSErrorDetailsProvider.class.getName()));

    Map<String, String> baseProperties = GCPUtils.getFileSystemProperties(config.connection,
      config.getPath(), new HashMap<>());
    if (config.getAllowFlexibleSchema()) {
      //Configure MultiSink with support for flexible schemas.
      configureSchemalessMultiSink(context, baseProperties, argumentCopy);
    } else {
      //Configure MultiSink with fixed schemas based on arguments.
      configureMultiSinkWithSchema(context, baseProperties, argumentCopy);
    }
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
  }

  private void configureMultiSinkWithSchema(BatchSinkContext context,
                                            Map<String, String> baseProperties,
                                            Map<String, String> argumentCopy)
    throws IOException, InstantiationException {

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
      outputProperties.put(GCSBatchSink.CONTENT_TYPE, config.getContentType());
      context.addOutput(Output.of(
        config.getReferenceName() + "_" + name,
        new SinkOutputFormatProvider(RecordFilterOutputFormat.class.getName(), outputProperties)));
    }
  }

  private void configureSchemalessMultiSink(BatchSinkContext context,
                                            Map<String, String> baseProperties,
                                            Map<String, String> argumentCopy) throws InstantiationException {
    ValidatingOutputFormat validatingOutputFormat = context.newPluginInstance(FORMAT_PLUGIN_ID);

    Map<String, String> outputProperties = new HashMap<>(baseProperties);
    outputProperties.putAll(validatingOutputFormat.getOutputFormatConfiguration());
    outputProperties.putAll(DelegatingGCSOutputFormat.configure(validatingOutputFormat.getOutputFormatClassName(),
                                                                config.splitField,
                                                                config.getOutputBaseDir(),
                                                                config.getOutputSuffix(context.getLogicalStartTime())));
    outputProperties.put(GCSBatchSink.CONTENT_TYPE, config.getContentType());
    context.addOutput(Output.of(
      config.getReferenceName(),
      new SinkOutputFormatProvider(DelegatingGCSOutputFormat.class.getName(), outputProperties)));
  }

  /**
   * Sink configuration.
   */
  public static class GCSMultiBatchSinkConfig extends GCSBatchSink.GCSBatchSinkConfig {
    private static final String NAME_ALLOW_FLEXIBLE_SCHEMA = "allowFlexibleSchema";

    @Description("The codec to use when writing data. " +
      "The 'avro' format supports 'snappy' and 'deflate'. The parquet format supports 'snappy' and 'gzip'. " +
      "Other formats do not support compression.")
    @Nullable
    private String compressionCodec;

    @Description("The name of the field that will be used to determine which directory to write to.")
    private String splitField = "tablename";

    @Name(NAME_ALLOW_FLEXIBLE_SCHEMA)
    @Macro
    @Nullable
    @Description("Allow Flexible Schemas in output. If disabled, only records with schemas set as " +
      "arguments will be processed. If enabled, all records will be written as-is.")
    private Boolean allowFlexibleSchema;

    protected String getOutputDir(long logicalStartTime, String context) {
      return String.format("%s/%s/%s", getOutputBaseDir(), context, getOutputSuffix(logicalStartTime));
    }

    protected String getOutputBaseDir() {
      return getPath();
    }

    protected String getOutputSuffix(long logicalStartTime) {
      boolean suffixOk = !Strings.isNullOrEmpty(getSuffix());
      return suffixOk ? new SimpleDateFormat(getSuffix()).format(logicalStartTime) : "";
    }

    public Boolean getAllowFlexibleSchema() {
      return allowFlexibleSchema != null ? allowFlexibleSchema : false;
    }
  }
}
