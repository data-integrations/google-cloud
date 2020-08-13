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
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.plugin.AbstractFileSink;
import io.cdap.plugin.format.plugin.FileSinkProperties;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import io.cdap.plugin.gcp.gcs.StorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Writes data to files on Google Cloud Storage.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCS")
@Description("Writes records to one or more files in a directory on Google Cloud Storage.")
public class GCSBatchSink extends AbstractFileSink<GCSBatchSink.GCSBatchSinkConfig> {

  private static final Logger LOG = LoggerFactory.getLogger(GCSBatchSink.class);
  public static final String RECORD_COUNT = "recordcount";
  private static final String RECORDS_UPDATED_METRIC = "records.updated";
  public static final String AVRO_NAMED_OUTPUT = "avro.mo.config.namedOutput";
  public static final String COMMON_NAMED_OUTPUT = "mapreduce.output.basename";

  private final GCSBatchSinkConfig config;
  private String outputPath;


  public GCSBatchSink(GCSBatchSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public ValidatingOutputFormat getValidatingOutputFormat(PipelineConfigurer pipelineConfigurer) {
    ValidatingOutputFormat delegate = super.getValidatingOutputFormat(pipelineConfigurer);
    return new GCSOutputFormatProvider(delegate);
  }

  @Override
  public ValidatingOutputFormat getOutputFormatForRun(BatchSinkContext context) throws InstantiationException {
    ValidatingOutputFormat outputFormatForRun = super.getOutputFormatForRun(context);
    return new GCSOutputFormatProvider(outputFormatForRun);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    super.prepareRun(context);
    String cmekKey = context.getArguments().get(GCPUtils.CMEK_KEY);
    Credentials credentials = config.getServiceAccountFilePath() == null ?
                                null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath());
    Storage storage = GCPUtils.getStorage(config.getProject(), credentials);
    Bucket bucket;
    try {
      bucket = storage.get(config.getBucket());
    } catch (StorageException e) {
      throw new RuntimeException(
        String.format("Unable to access or create bucket %s. ", config.getBucket())
          + "Ensure you entered the correct bucket path and have permissions for it.", e);
    }
    if (bucket == null) {
      GCPUtils.createBucket(storage, config.getBucket(), config.getLocation(), cmekKey);
    }
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSinkContext context) {
    Map<String, String> properties = GCPUtils.getFileSystemProperties(config, config.getPath(), new HashMap<>());
    properties.putAll(config.getFileSystemProperties());
    String outputFileBaseName = config.getOutputFileNameBase();
    if (outputFileBaseName == null || outputFileBaseName.isEmpty()) {
      return properties;
    }

    properties.put(AVRO_NAMED_OUTPUT, outputFileBaseName);
    properties.put(COMMON_NAMED_OUTPUT, outputFileBaseName);
    return properties;
  }

  @Override
  protected String getOutputDir(long logicalStartTime) {
    this.outputPath = super.getOutputDir(logicalStartTime);
    return this.outputPath;
  }

  @Override
  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordWrite("Write", "Wrote to Google Cloud Storage.", outputFields);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    super.onRunFinish(succeeded, context);
    emitMetrics(succeeded, context);
  }

  private void emitMetrics(boolean succeeded, BatchSinkContext context) {
    if (!succeeded) {
      return;
    }

    try {
      StorageClient storageClient = StorageClient.create(config.getProject(), config.getServiceAccountFilePath());
      storageClient.mapMetaDataForAllBlobs(getPrefixPath(), new MetricsEmitter(context.getMetrics())::emitMetrics);
    } catch (Exception e) {
      LOG.warn("Metrics for the number of affected rows in GCS Sink maybe incorrect.", e);
    }
  }

  private String getPrefixPath() {
    String filenameBase = getFilenameBase();
    if (filenameBase == null) {
      return outputPath;
    }
    //Following code is for handling a special case for saving files in same output directory.
    //The specified file prefix from Filesystem Properties/Output file base name can be used to make filename unique.
    //The code is based on assumptions from the internal implementation of
    //org.apache.avro.mapreduce.AvroOutputFormatBase and org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
    String outputPathPrefix = outputPath.endsWith("/") ? outputPath.substring(0, outputPath.length() - 1) : outputPath;
    return String.format("%s/%s-", outputPathPrefix, filenameBase);
  }

  @Nullable
  private String getFilenameBase() {
    String outputFileBaseName = config.getOutputFileNameBase();
    if (outputFileBaseName != null && !outputFileBaseName.isEmpty()) {
      return outputFileBaseName;
    }

    Map<String, String> fileSystemProperties = config.getFileSystemProperties();
    if (fileSystemProperties.containsKey(AVRO_NAMED_OUTPUT) && config.getFormat() == FileFormat.AVRO) {
      return fileSystemProperties.get(AVRO_NAMED_OUTPUT);
    }
    if (fileSystemProperties.containsKey(COMMON_NAMED_OUTPUT)) {
      return fileSystemProperties.get(COMMON_NAMED_OUTPUT);
    }
    return null;
  }

  private static class MetricsEmitter {
    private StageMetrics stageMetrics;

    private MetricsEmitter(StageMetrics stageMetrics) {
      this.stageMetrics = stageMetrics;
    }

    public void emitMetrics(Map<String, String> metaData) {
      long totalRows = extractRecordCount(metaData);
      if (totalRows == 0) {
        return;
      }

      //work around since StageMetrics count() only takes int as of now
      int cap = 10000; // so the loop will not cause significant delays
      long count = totalRows / Integer.MAX_VALUE;
      if (count > cap) {
        LOG.warn("Total record count is too high! Metric for the number of affected rows may not be updated correctly");
      }
      count = count < cap ? count : cap;
      for (int i = 0; i <= count && totalRows > 0; i++) {
        int rowCount = totalRows < Integer.MAX_VALUE ? (int) totalRows : Integer.MAX_VALUE;
        stageMetrics.count(RECORDS_UPDATED_METRIC, rowCount);
        totalRows -= rowCount;
      }
    }

    private long extractRecordCount(Map<String, String> metadata) {
      String value = metadata.get(RECORD_COUNT);
      return value == null ? 0L : Long.parseLong(value);
    }
  }

  /**
   * Sink configuration.
   */
  @SuppressWarnings("unused")
  public static class GCSBatchSinkConfig extends GCPReferenceSinkConfig implements FileSinkProperties {
    private static final String NAME_PATH = "path";
    private static final String NAME_SUFFIX = "suffix";
    private static final String NAME_FORMAT = "format";
    private static final String NAME_SCHEMA = "schema";
    private static final String NAME_LOCATION = "location";
    private static final String NAME_FS_PROPERTIES = "fileSystemProperties";
    private static final String NAME_FILE_NAME_BASE = "outputFileNameBase";

    private static final String SCHEME = "gs://";
    @Name(NAME_PATH)
    @Description("The path to write to. For example, gs://<bucket>/path/to/directory")
    @Macro
    private String path;

    @Description("The time format for the output directory that will be appended to the path. " +
      "For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. " +
      "If not specified, nothing will be appended to the path.")
    @Nullable
    @Macro
    private String suffix;

    @Macro
    @Description("The format to write in. The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', "
      + "or 'delimited'.")
    protected String format;

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

    @Name(NAME_LOCATION)
    @Macro
    @Nullable
    @Description("The location where the gcs bucket will get created. " +
                   "This value is ignored if the bucket already exists")
    protected String location;

    @Name(NAME_FS_PROPERTIES)
    @Macro
    @Nullable
    @Description("Advanced feature to specify any additional properties that should be used with the sink.")
    private String fileSystemProperties;

    @Name(NAME_FILE_NAME_BASE)
    @Macro
    @Nullable
    @Description("Advanced feature to specify file output name prefix.")
    private String outputFileNameBase;

    @Override
    public void validate() {
      // no-op
    }

    @Override
    public void validate(FailureCollector collector) {
      super.validate(collector);
      // validate that path is valid
      if (!containsMacro(NAME_PATH)) {
        try {
          GCSPath.from(path);
        } catch (IllegalArgumentException e) {
          collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_PATH).withStacktrace(e.getStackTrace());
        }
      }
      if (suffix != null && !containsMacro(NAME_SUFFIX)) {
        try {
          new SimpleDateFormat(suffix);
        } catch (IllegalArgumentException e) {
          collector.addFailure("Invalid suffix : " + e.getMessage(), null)
            .withConfigProperty(NAME_SUFFIX).withStacktrace(e.getStackTrace());
        }
      }

      if (!containsMacro(NAME_FORMAT)) {
        try {
          getFormat();
        } catch (IllegalArgumentException e) {
          collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_FORMAT)
            .withStacktrace(e.getStackTrace());
        }
      }

      try {
        getSchema();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_SCHEMA).withStacktrace(e.getStackTrace());
      }

      try {
        getFileSystemProperties();
      } catch (IllegalArgumentException e) {
        collector.addFailure("File system properties must be a valid json.", null)
          .withConfigProperty(NAME_FS_PROPERTIES).withStacktrace(e.getStackTrace());
      }
    }

    public String getBucket() {
      return GCSPath.from(path).getBucket();
    }

    @Override
    public String getPath() {
      GCSPath gcsPath = GCSPath.from(path);
      return SCHEME + gcsPath.getBucket() + gcsPath.getUri().getPath();
    }

    @Override
    public FileFormat getFormat() {
      return FileFormat.from(format, FileFormat::canWrite);
    }

    @Nullable
    public Schema getSchema() {
      if (containsMacro("schema") || Strings.isNullOrEmpty(schema)) {
        return null;
      }
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
      }
    }

    @Nullable
    @Override
    public String getSuffix() {
      return suffix;
    }

    @Nullable
    public String getDelimiter() {
      return delimiter;
    }

    @Nullable
    public String getLocation() {
      return location;
    }

    public Map<String, String> getFileSystemProperties() {
      if (fileSystemProperties == null || fileSystemProperties.isEmpty()) {
        return Collections.emptyMap();
      }
      try {
        return new Gson().fromJson(fileSystemProperties, new TypeToken<Map<String, String>>() {
        }.getType());
      } catch (JsonSyntaxException e) {
        throw new IllegalArgumentException("Unable to parse filesystem properties: " + e.getMessage(), e);
      }
    }

    @Nullable
    public String getOutputFileNameBase() {
      return outputFileNameBase;
    }
  }
}
