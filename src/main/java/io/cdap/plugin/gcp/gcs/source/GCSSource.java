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

package io.cdap.plugin.gcp.gcs.source;

import com.google.auth.Credentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import io.cdap.plugin.format.plugin.AbstractFileSource;
import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;
import io.cdap.plugin.format.plugin.FileSourceProperties;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.common.GCSEmptyInputFormat;
import io.cdap.plugin.gcp.crypto.EncryptedFileSystem;
import io.cdap.plugin.gcp.gcs.GCSPath;
import io.cdap.plugin.gcp.gcs.connector.GCSConnector;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Class description here.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(GCSSource.NAME)
@Description("Reads objects from a path in a Google Cloud Storage bucket.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = GCSConnector.NAME)})
public class GCSSource extends AbstractFileSource<GCSSource.GCSSourceConfig> {
  public static final String NAME = "GCSFile";
  private final GCSSourceConfig config;
  private Asset asset;

  public GCSSource(GCSSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  protected String getEmptyInputFormatClassName() {
    return GCSEmptyInputFormat.class.getName();
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    // Get location of the source for lineage
    String location;
    String bucketName = GCSPath.from(config.getPath()).getBucket();
    Credentials credentials = config.connection.getServiceAccount() == null ?
      null : GCPUtils.loadServiceAccountCredentials(config.connection.getServiceAccount(),
                                                    config.connection.isServiceAccountFilePath());
    Storage storage = GCPUtils.getStorage(config.connection.getProject(), credentials);
    try {
      location = storage.get(bucketName).getLocation();
    } catch (StorageException e) {
      throw new RuntimeException(
        String.format("Unable to access bucket %s. ", bucketName)
          + "Ensure you entered the correct bucket path and have permissions for it.", e);
    }

    // create asset for lineage
    String fqn = GCSPath.getFQN(config.getPath());
    String referenceName = Strings.isNullOrEmpty(config.getReferenceName())
        ? ReferenceNames.normalizeFqn(fqn)
        : config.getReferenceName();
    asset = Asset.builder(referenceName)
        .setFqn(fqn).setLocation(location).build();

    // super is called down here to avoid instantiating the lineage recorder with a null asset
    super.prepareRun(context);
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    Map<String, String> properties = GCPUtils.getFileSystemProperties(config.connection, config.getPath(),
                                                                      new HashMap<>(config.getFileSystemProperties()));
    if (config.isCopyHeader()) {
      properties.put(PathTrackingInputFormat.COPY_HEADER, Boolean.TRUE.toString());
    }
    if (config.getFileEncoding() != null
      && !config.getFileEncoding().equalsIgnoreCase(AbstractFileSourceConfig.DEFAULT_FILE_ENCODING)) {
      properties.put(PathTrackingInputFormat.SOURCE_FILE_ENCODING, config.getFileEncoding());
    }
    if (config.getMinSplitSize() != null) {
      properties.put("mapreduce.input.fileinputformat.split.minsize", String.valueOf(config.getMinSplitSize()));
    }
    if (config.isEncrypted()) {
      TinkDecryptor.configure(config.getEncryptedMetadataSuffix(), properties);
      EncryptedFileSystem.configure("gs", TinkDecryptor.class, properties);
      GCSRegexPathFilter.configure(config, properties);
    }

    return properties;
  }

  @Override
  protected LineageRecorder getLineageRecorder(BatchSourceContext context) {
    return new LineageRecorder(context, asset);
  }

  @Override
  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordRead("Read", String.format("Read%sfrom Google Cloud Storage.",
                                                     config.isEncrypted() ? " and decrypt " : " "), outputFields);
  }

  @Override
  protected boolean shouldGetSchema() {
    return !config.containsMacro(GCPConnectorConfig.NAME_PROJECT) &&
             !config.containsMacro(GCSSourceConfig.NAME_PATH) && !config.containsMacro(GCSSourceConfig.NAME_FORMAT) &&
             !config.containsMacro(GCSSourceConfig.NAME_DELIMITER) &&
             !config.containsMacro(GCSSourceConfig.NAME_FILE_SYSTEM_PROPERTIES) &&
             !config.containsMacro(GCPConnectorConfig.NAME_SERVICE_ACCOUNT_FILE_PATH) &&
             !config.containsMacro(GCPConnectorConfig.NAME_SERVICE_ACCOUNT_JSON);
  }

  /**
   * Config for the plugin.
   */
  @SuppressWarnings("ConstantConditions")
  public static class GCSSourceConfig extends AbstractFileSourceConfig implements FileSourceProperties {
    public static final String NAME_PATH = "path";
    public static final String NAME_FORMAT = "format";
    private static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";
    private static final String NAME_FILE_REGEX = "fileRegex";
    private static final String NAME_DELIMITER = "delimiter";
    private static final String NAME_SHEET = "sheet";
    private static final String NAME_SHEET_VALUE = "sheetValue";
    private static final String NAME_TERMINATE_IF_EMPTY_ROW = "terminateIfEmptyRow";

    private static final String DEFAULT_ENCRYPTED_METADATA_SUFFIX = ".metadata";

    private static final Gson GSON = new Gson();
    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    @Macro
    @Description("The path to read from. For example, gs://<bucket>/path/to/directory/")
    private String path;

    @Macro
    @Nullable
    @Description("Map of properties to set on the InputFormat.")
    private String fileSystemProperties;

    @Macro
    @Nullable
    @Description("Minimum size of each partition used to read data. ")
    private Long minSplitSize;

    @Macro
    @Nullable
    @Description("Whether the data file is encrypted. If it is set to 'true', a associated metadata file needs to be "
      + "provided for each data file. Please refer to the Documentation for the details of the metadata file content.")
    private Boolean encrypted;

    @Macro
    @Nullable
    @Description("The file name suffix for the metadata file of the encrypted data file. "
      + "The default is '" + DEFAULT_ENCRYPTED_METADATA_SUFFIX + "'.")
    private String encryptedMetadataSuffix;

    @Macro
    @Nullable
    @Description("A list of columns with the corresponding data types for whom the automatic data type detection gets" +
      " skipped.")
    private String override;

    @Macro
    @Nullable
    @Description("The maximum number of rows that will get investigated for automatic data type detection.")
    private Long sampleSize;

    @Name(ConfigUtil.NAME_USE_CONNECTION)
    @Nullable
    @Description("Whether to use an existing connection.")
    private Boolean useConnection;

    @Name(ConfigUtil.NAME_CONNECTION)
    @Macro
    @Nullable
    @Description("The existing connection to use.")
    private GCPConnectorConfig connection;

    @Name(NAME_SHEET)
    @Macro
    @Nullable
    @Description("Select the sheet by name or number. Default is 'Sheet Number'.")
    private String sheet;

    @Name(NAME_SHEET_VALUE)
    @Macro
    @Nullable
    @Description("The name/number of the sheet to read from. If not specified, the first sheet will be read." +
            "Sheet Number are 0 based, ie first sheet is 0.")
    private String sheetValue;

    @Name(NAME_TERMINATE_IF_EMPTY_ROW)
    @Macro
    @Nullable
    @Description("Specify whether to stop reading after encountering the first empty row. Defaults to false.")
    private String terminateIfEmptyRow;

    @Override
    public void validate() {
      // no-op
    }

    public void validate(FailureCollector collector) {
      super.validate(collector);
      ConfigUtil.validateConnection(this, useConnection, connection, collector);
      // validate that path is valid
      if (!containsMacro(NAME_PATH)) {
        try {
          GCSPath.from(path);
        } catch (IllegalArgumentException e) {
          collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_PATH)
            .withStacktrace(e.getStackTrace());
        }
      }
      if (!containsMacro(NAME_FILE_SYSTEM_PROPERTIES)) {
        try {
          getFileSystemProperties();
        } catch (Exception e) {
          collector.addFailure("File system properties must be a valid json.", null)
            .withConfigProperty(NAME_FILE_SYSTEM_PROPERTIES).withStacktrace(e.getStackTrace());
        }
      }
      if (!containsMacro(NAME_FILE_REGEX)) {
        try {
          getFilePattern();
        } catch (IllegalArgumentException e) {
          collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_FILE_REGEX)
            .withStacktrace(e.getStackTrace());
        }
      }
    }

    @Override
    public String getPath() {
      return path;
    }

    @Nullable
    public Pattern getExclusionPattern() {
      if (!isEncrypted()) {
        return null;
      }

      return Pattern.compile(".*" + Pattern.quote(getEncryptedMetadataSuffix()) + "$");
    }

    @Nullable
    public Long getMinSplitSize() {
      return minSplitSize;
    }

    public boolean isCopyHeader() {
      return shouldCopyHeader();
    }

    public boolean isEncrypted() {
      return encrypted != null && encrypted;
    }

    public String getEncryptedMetadataSuffix() {
      return Strings.isNullOrEmpty(encryptedMetadataSuffix) ?
        DEFAULT_ENCRYPTED_METADATA_SUFFIX : encryptedMetadataSuffix;
    }

    Map<String, String> getFileSystemProperties() {
      if (fileSystemProperties == null) {
        return Collections.emptyMap();
      }
      return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
  }
}
