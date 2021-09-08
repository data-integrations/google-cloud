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
  public static final String CONTENT_TYPE = "io.cdap.gcs.batch.sink.content.type";

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
    cmekKey = config.getCmekKey(cmekKey);

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
    properties.put(GCSBatchSink.CONTENT_TYPE, config.getContentType());
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
      StorageClient storageClient = StorageClient.create(config.getProject(), config.getServiceAccount(),
                                                         config.isServiceAccountFilePath());
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
    private static final String NAME_CONTENT_TYPE = "contentType";
    private static final String NAME_CUSTOM_CONTENT_TYPE = "customContentType";
    private static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    private static final String CONTENT_TYPE_OTHER = "other";
    private static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";
    private static final String CONTENT_TYPE_APPLICATION_AVRO = "application/avro";
    private static final String CONTENT_TYPE_APPLICATION_CSV = "application/csv";
    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";
    private static final String CONTENT_TYPE_TEXT_CSV = "text/csv";
    private static final String CONTENT_TYPE_TEXT_TSV = "text/tab-separated-values";
    private static final String FORMAT_AVRO = "avro";
    private static final String FORMAT_CSV = "csv";
    private static final String FORMAT_JSON = "json";
    private static final String FORMAT_TSV = "tsv";
    private static final String FORMAT_DELIMITED = "delimited";
    private static final String FORMAT_ORC = "orc";
    private static final String FORMAT_PARQUET = "parquet";
    private static final String NAME_CMEK_KEY = "cmekKey";

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

    @Macro
    @Nullable
    @Description("Whether a header should be written to each output file. This only applies to the delimited, csv, " +
      "and tsv formats.")
    private Boolean writeHeader;

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

    @Macro
    @Description("The Content Type property is used to indicate the media type of the resource." +
      "Defaults to 'application/octet-stream'.")
    @Nullable
    protected String contentType;

    @Macro
    @Description("The Custom Content Type is used when the value of Content-Type is set to other." +
      "User can provide specific Content-Type, different from the options in the dropdown.")
    @Nullable
    protected String customContentType;

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

    @Name(NAME_CMEK_KEY)
    @Macro
    @Nullable
    @Description("The GCP customer managed encryption key (CMEK) name used by Cloud Dataproc")
    private String cmekKey;

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
          collector.addFailure("Invalid suffix.", "Ensure provided suffix is valid.")
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

      if (!containsMacro(NAME_CONTENT_TYPE) && !containsMacro(NAME_CUSTOM_CONTENT_TYPE)
        && !Strings.isNullOrEmpty(contentType) && !contentType.equalsIgnoreCase(CONTENT_TYPE_OTHER)
        && !containsMacro(NAME_FORMAT)) {
        if (!contentType.equalsIgnoreCase(DEFAULT_CONTENT_TYPE)) {
          validateContentType(collector);
        }
      }

      if (!containsMacro(NAME_CMEK_KEY) && !Strings.isNullOrEmpty(cmekKey)) {
        validateCmekKey(collector);
      }

      try {
        getSchema();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_SCHEMA)
          .withStacktrace(e.getStackTrace());
      }

      try {
        getFileSystemProperties();
      } catch (IllegalArgumentException e) {
        collector.addFailure("File system properties must be a valid json.", null)
          .withConfigProperty(NAME_FS_PROPERTIES).withStacktrace(e.getStackTrace());
      }
    }

    //This method validated the pattern of CMEK Key resource ID.
    private void validateCmekKey(FailureCollector failureCollector) {
      String[] part = cmekKey.split("/");
      if ((part.length != 8) || !part[0].equals("projects") || !part[2].equals("locations")
        || !part[4].equals("keyRings") || !part[6].equals("cryptoKeys")) {
        failureCollector.addFailure("Invalid cmek key resource Id format. It should be of the form " +
                                      "projects/<gcp-project-id>/locations/<key-location>/keyRings/" +
                                      "<key-ring-name>/cryptoKeys/<key-name>", null)
          .withConfigProperty(NAME_CMEK_KEY);
      } else if (!part[3].equals(location)) {
        failureCollector.addFailure(String.format("Cloud KMS region '%s' not available for use " +
                                                    "with GCS region '%s'.", part[3], location), null)
          .withConfigProperty(NAME_CMEK_KEY);
      }
    }

    public String getCmekKey(@Nullable String key) {
      if (Strings.isNullOrEmpty(cmekKey)) {
        return key;
      }
      return cmekKey;
    }

    //This method validates the specified content type for the used format.
    public void validateContentType(FailureCollector failureCollector) {
      switch (format) {
        case FORMAT_AVRO:
          if (!contentType.equalsIgnoreCase(CONTENT_TYPE_APPLICATION_AVRO)) {
            failureCollector.addFailure(String.format("Valid content types for avro are %s, %s.",
                                                      CONTENT_TYPE_APPLICATION_AVRO, DEFAULT_CONTENT_TYPE), null)
              .withConfigProperty(NAME_CONTENT_TYPE);
          }
          break;
        case FORMAT_JSON:
          if (!contentType.equalsIgnoreCase(CONTENT_TYPE_APPLICATION_JSON)
            && !contentType.equalsIgnoreCase(CONTENT_TYPE_TEXT_PLAIN)) {
            failureCollector.addFailure(String.format(
              "Valid content types for json are %s, %s, %s.", CONTENT_TYPE_APPLICATION_JSON,
              CONTENT_TYPE_TEXT_PLAIN, DEFAULT_CONTENT_TYPE), null
            ).withConfigProperty(NAME_CONTENT_TYPE);
          }
          break;
        case FORMAT_CSV:
          if (!contentType.equalsIgnoreCase(CONTENT_TYPE_APPLICATION_CSV)
            && !contentType.equalsIgnoreCase(CONTENT_TYPE_TEXT_CSV)
            && !contentType.equalsIgnoreCase(CONTENT_TYPE_TEXT_PLAIN)) {
            failureCollector.addFailure(String.format(
              "Valid content types for csv are %s, %s, %s, %s.", CONTENT_TYPE_APPLICATION_CSV,
              CONTENT_TYPE_TEXT_PLAIN, CONTENT_TYPE_TEXT_CSV, DEFAULT_CONTENT_TYPE), null
            ).withConfigProperty(NAME_CONTENT_TYPE);
          }
          break;
        case FORMAT_DELIMITED:
          if (!contentType.equalsIgnoreCase(CONTENT_TYPE_TEXT_PLAIN)
            && !contentType.equalsIgnoreCase(CONTENT_TYPE_TEXT_CSV)
            && !contentType.equalsIgnoreCase(CONTENT_TYPE_APPLICATION_CSV)
            && !contentType.equalsIgnoreCase(CONTENT_TYPE_TEXT_TSV)) {
            failureCollector.addFailure(String.format(
              "Valid content types for delimited are %s, %s, %s, %s, %s.", CONTENT_TYPE_TEXT_PLAIN,
              CONTENT_TYPE_TEXT_CSV, CONTENT_TYPE_APPLICATION_CSV, CONTENT_TYPE_TEXT_TSV, DEFAULT_CONTENT_TYPE), null
            ).withConfigProperty(NAME_CONTENT_TYPE);
          }
          break;
        case FORMAT_PARQUET:
          if (!contentType.equalsIgnoreCase(DEFAULT_CONTENT_TYPE)) {
            failureCollector.addFailure(String.format("Valid content type for parquet is %s.", DEFAULT_CONTENT_TYPE),
                                        null).withConfigProperty(NAME_CONTENT_TYPE);
          }
          break;
        case FORMAT_ORC:
          if (!contentType.equalsIgnoreCase(DEFAULT_CONTENT_TYPE)) {
            failureCollector.addFailure(String.format("Valid content type for orc is %s.", DEFAULT_CONTENT_TYPE),
                                        null).withConfigProperty(NAME_CONTENT_TYPE);
          }
          break;
        case FORMAT_TSV:
          if (!contentType.equalsIgnoreCase(CONTENT_TYPE_TEXT_PLAIN)
            && !contentType.equalsIgnoreCase(CONTENT_TYPE_TEXT_TSV)) {
            failureCollector.addFailure(String.format(
              "Valid content types for tsv are %s, %s, %s.", CONTENT_TYPE_TEXT_TSV, CONTENT_TYPE_TEXT_PLAIN,
              DEFAULT_CONTENT_TYPE), null).withConfigProperty(NAME_CONTENT_TYPE);
          }
          break;
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

    /*  This method gets the value of content type. Valid content types for each format are:
     *
     *  avro -> application/avro, application/octet-stream
     *  json -> application/json, text/plain, application/octet-stream
     *  csv -> application/csv, text/csv, text/plain, application/octet-stream
     *  delimited -> application/csv, text/csv, text/plain, text/tsv, application/octet-stream
     *  orc -> application/octet-stream
     *  parquet -> application/octet-stream
     *  tsv -> text/tab-separated-values, application/octet-stream
     */
    @Nullable
    public String getContentType() {
      if (!Strings.isNullOrEmpty(contentType)) {
        if (contentType.equals(CONTENT_TYPE_OTHER)) {
          if (Strings.isNullOrEmpty(customContentType)) {
            return DEFAULT_CONTENT_TYPE;
          }
          return customContentType;
        }
        return contentType;
      }
      return DEFAULT_CONTENT_TYPE;
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
