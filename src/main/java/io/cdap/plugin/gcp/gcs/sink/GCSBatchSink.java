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
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProviderSpec;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.plugin.AbstractFileSink;
import io.cdap.plugin.format.plugin.FileSinkProperties;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.Formats;
import io.cdap.plugin.gcp.gcs.GCSErrorDetailsProvider;
import io.cdap.plugin.gcp.gcs.GCSPath;
import io.cdap.plugin.gcp.gcs.StorageClient;
import io.cdap.plugin.gcp.gcs.connector.GCSConnector;
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
@Name(GCSBatchSink.NAME)
@Description("Writes records to one or more files in a directory on Google Cloud Storage.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = GCSConnector.NAME)})
public class GCSBatchSink extends AbstractFileSink<GCSBatchSink.GCSBatchSinkConfig> {

  public static final String NAME = "GCS";
  private static final Logger LOG = LoggerFactory.getLogger(GCSBatchSink.class);
  public static final String RECORD_COUNT = "recordcount";
  private static final String RECORDS_UPDATED_METRIC = "records.updated";
  public static final String AVRO_NAMED_OUTPUT = "avro.mo.config.namedOutput";
  public static final String COMMON_NAMED_OUTPUT = "mapreduce.output.basename";
  public static final String CONTENT_TYPE = "io.cdap.gcs.batch.sink.content.type";

  private final GCSBatchSinkConfig config;
  private String outputPath;
  private Asset asset;

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
    FailureCollector collector = context.getFailureCollector();
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
    Bucket bucket;
    String location = null;
    try {
      bucket = storage.get(bucketName);
      if (bucket != null) {
        location = bucket.getLocation();
      } else {
        location = config.getLocation();
        GCPUtils.createBucket(storage, bucketName, location, cmekKeyName);
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

    this.outputPath = getOutputDir(context);
    // create asset for lineage
    asset = Asset.builder(config.getReferenceName())
      .setFqn(GCSPath.getFQN(config.getPath())).setLocation(location).build();

    // set error details provider
    context.setErrorDetailsProvider(
      new ErrorDetailsProviderSpec(GCSErrorDetailsProvider.class.getName()));
    
    // super is called down here to avoid instantiating the lineage recorder with a null asset
    super.prepareRun(context);
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSinkContext context) {
    Map<String, String> properties = GCPUtils.getFileSystemProperties(config.connection, config.getPath(),
                                                                      new HashMap<>());
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
  protected LineageRecorder getLineageRecorder(BatchSinkContext context) {
    return new LineageRecorder(context, asset);
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
      StorageClient storageClient = StorageClient.create(config.connection);
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
    if (fileSystemProperties.containsKey(AVRO_NAMED_OUTPUT) &&
      FileFormat.AVRO.name().toLowerCase().equals(config.getFormatName())) {
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
  public static class GCSBatchSinkConfig extends PluginConfig implements FileSinkProperties {
    public static final String NAME_PATH = "path";
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
    public static final String NAME_CMEK_KEY = "cmekKey";

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
    @Description("The GCP customer managed encryption key (CMEK) name used to encrypt data written to " +
      "any bucket created by the plugin. If the bucket already exists, this is ignored. More information can be found" +
      " at https://cloud.google.com/data-fusion/docs/how-to/customer-managed-encryption-keys")
    protected String cmekKey;

    @Name(Constants.Reference.REFERENCE_NAME)
    @Nullable
    @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
    public String referenceName;

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    protected GCPConnectorConfig connection;

    @Override
    public void validate() {
      // no-op
    }

    @Override
    public void validate(FailureCollector collector) {
      validate(collector, Collections.emptyMap());
    }

    @Override
    public void validate(FailureCollector collector, Map<String, String> arguments) {
      if (!Strings.isNullOrEmpty(referenceName)) {
        IdUtils.validateReferenceName(referenceName, collector);
      }
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
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

      if (!containsMacro(NAME_CONTENT_TYPE) && !containsMacro(NAME_CUSTOM_CONTENT_TYPE)
        && !Strings.isNullOrEmpty(contentType) && !contentType.equalsIgnoreCase(CONTENT_TYPE_OTHER)
        && !containsMacro(NAME_FORMAT)) {
        if (!contentType.equalsIgnoreCase(DEFAULT_CONTENT_TYPE)) {
          validateContentType(collector);
        }
      }

      if (!containsMacro(NAME_CMEK_KEY)) {
        validateCmekKey(collector, arguments);
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

    /**
     * Return reference name if provided, otherwise, normalize the FQN and return it as reference name
     * @return referenceName (if provided)/normalized FQN
     */
    @Override
    public String getReferenceName() {
      return Strings.isNullOrEmpty(referenceName) ? ReferenceNames.normalizeFqn(
          GCSPath.getFQN(getPath())) : referenceName;
    }

    public GCSBatchSinkConfig(@Nullable String referenceName, @Nullable String project,
                              @Nullable String fileSystemProperties, @Nullable String serviceAccountType,
                              @Nullable String serviceFilePath, @Nullable String serviceAccountJson,
                              @Nullable String path, @Nullable String location, @Nullable String cmekKey,
                              @Nullable String format, @Nullable String contentType,
                              @Nullable String customContentType) {
      super();
      this.referenceName = referenceName;
      this.fileSystemProperties = fileSystemProperties;
      this.connection = new GCPConnectorConfig(project, serviceAccountType, serviceFilePath, serviceAccountJson);
      this.path = path;
      this.location = location;
      this.cmekKey = cmekKey;
      this.format = format;
      this.contentType = contentType;
      this.customContentType = customContentType;
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

    /**
     * Get the bucket name from the path.
     * @param collector failure collector
     * @return bucket name as {@link String} if found, otherwise null.
     */
    public String getBucket(FailureCollector collector) {
      try {
        return GCSPath.from(path).getBucket();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null)
          .withStacktrace(e.getStackTrace());
        collector.getOrThrowException();
      }
      return null;
    }

    @Override
    public String getPath() {
      GCSPath gcsPath = GCSPath.from(path);
      return SCHEME + gcsPath.getBucket() + gcsPath.getUri().getPath();
    }

    @Override
    public String getFormatName() {
      return Formats.getFormatPluginName(format);
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

    public GCSBatchSinkConfig() {
      super();
    }

    //This method validated the pattern of CMEK Key resource ID.
    void validateCmekKey(FailureCollector failureCollector, Map<String, String> arguments) {
      CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(cmekKey, arguments, failureCollector);

      //these fields are needed to check if bucket exists or not and for location validation
      if (cmekKeyName == null || containsMacro(NAME_PATH) || containsMacro(NAME_LOCATION) || connection == null
        || !connection.canConnect()) {
        return;
      }

      Storage storage = GCPUtils.getStorage(connection.getProject(), connection.getCredentials(failureCollector));
      if (storage == null) {
        return;
      }
      CmekUtils.validateCmekKeyAndBucketLocation(storage, GCSPath.from(path), cmekKeyName, location, failureCollector);
    }

    public static Builder builder() {
      return new Builder();
    }

    /**
     * GCS Batch Sink configuration builder.
     */
    public static class Builder {
      private String referenceName;
      private String serviceAccountType;
      private String serviceFilePath;
      private String serviceAccountJson;
      private String fileSystemProperties;
      private String project;
      private String gcsPath;
      private String cmekKey;
      private String location;
      private String format;
      private String contentType;
      private String customContentType;

      public Builder setReferenceName(@Nullable String referenceName) {
        this.referenceName = referenceName;
        return this;
      }

      public Builder setProject(@Nullable String project) {
        this.project = project;
        return this;
      }

      public Builder setServiceAccountType(@Nullable String serviceAccountType) {
        this.serviceAccountType = serviceAccountType;
        return this;
      }

      public Builder setServiceFilePath(@Nullable String serviceFilePath) {
        this.serviceFilePath = serviceFilePath;
        return this;
      }

      public Builder setServiceAccountJson(@Nullable String serviceAccountJson) {
        this.serviceAccountJson = serviceAccountJson;
        return this;
      }

      public Builder setGcsPath(@Nullable String gcsPath) {
        this.gcsPath = gcsPath;
        return this;
      }

      public Builder setCmekKey(@Nullable String cmekKey) {
        this.cmekKey = cmekKey;
        return this;
      }

      public Builder setLocation(@Nullable String location) {
        this.location = location;
        return this;
      }

      public Builder setFileSystemProperties(@Nullable String fileSystemProperties) {
        this.fileSystemProperties = fileSystemProperties;
        return this;
      }

      public Builder setFormat(@Nullable String format) {
        this.format = format;
        return this;
      }

      public Builder setContentType(@Nullable String contentType) {
        this.contentType = contentType;
        return this;
      }

      public Builder setCustomContentType(@Nullable String customContentType) {
        this.customContentType = customContentType;
        return this;
      }

      public GCSBatchSinkConfig build() {
        return new GCSBatchSinkConfig(
          referenceName,
          project,
          fileSystemProperties,
          serviceAccountType,
          serviceFilePath,
          serviceAccountJson,
          gcsPath,
          location,
          cmekKey,
          format,
          contentType,
          customContentType
        );
      }

    }
  }
}
