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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import io.cdap.plugin.format.plugin.AbstractFileSource;
import io.cdap.plugin.format.plugin.FileSourceProperties;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.crypto.EncryptedFileSystem;
import io.cdap.plugin.gcp.gcs.GCSPath;

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
public class GCSSource extends AbstractFileSource<GCSSource.GCSSourceConfig> {
  public static final String NAME = "GCSFile";
  private final GCSSourceConfig config;

  public GCSSource(GCSSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    Map<String, String> properties = GCPUtils.getFileSystemProperties(config, config.getPath(),
                                                                      new HashMap<>(config.getFileSystemProperties()));
    if (config.isCopyHeader()) {
      properties.put(PathTrackingInputFormat.COPY_HEADER, Boolean.TRUE.toString());
    }

    if (config.isEncrypted()) {
      TinkDecryptor.configure(config.getEncryptedMetadataSuffix(), properties);
      EncryptedFileSystem.configure("gs", TinkDecryptor.class, properties);
      GCSRegexPathFilter.configure(config, properties);
    }

    return properties;
  }

  @Override
  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordRead("Read", String.format("Read%sfrom Google Cloud Storage.",
                                                     config.isEncrypted() ? " and decrypt " : " "), outputFields);
  }

  /**
   * Config for the plugin.
   */
  @SuppressWarnings("ConstantConditions")
  public static class GCSSourceConfig extends GCPReferenceSourceConfig implements FileSourceProperties {
    private static final String NAME_PATH = "path";
    private static final String NAME_FILE_SYSTEM_PROPERTIES = "fileSystemProperties";
    private static final String NAME_FILE_REGEX = "fileRegex";
    private static final String NAME_FORMAT = "format";

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
    @Description("Maximum size of each partition used to read data. "
      + "Smaller partitions will increase the level of parallelism, but will require more resources and overhead.")
    private Long maxSplitSize;

    @Macro
    @Nullable
    @Description("Output field to place the path of the file that the record was read from. "
      + "If not specified, the file path will not be included in output records. "
      + "If specified, the field must exist in the output schema as a string.")
    private String pathField;

    @Macro
    @Description("Format of the data to read. Supported formats are 'avro', 'blob', 'csv', 'delimited', 'json', "
      + "'parquet', 'text', and 'tsv'.")
    private String format;

    @Macro
    @Nullable
    @Description("Output schema. If a Path Field is set, it must be present in the schema as a string.")
    private String schema;

    @Macro
    @Nullable
    @Description("Whether to only use the filename instead of the URI of the file path when a path field is given. "
      + "The default value is false.")
    private Boolean filenameOnly;

    @Macro
    @Nullable
    @Description("Regular expression that file paths must match in order to be included in the input. "
      + "The full file path is compared, not just the file name."
      + "If no value is given, no file filtering will be done. "
      + "See https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html for more information about "
      + "the regular expression syntax.")
    private String fileRegex;

    @Macro
    @Nullable
    @Description("Whether to recursively read directories within the input directory. The default is false.")
    private Boolean recursive;

    @Macro
    @Nullable
    @Description("The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the format "
      + "is anything other than 'delimited'.")
    private String delimiter;

    @Macro
    @Nullable
    @Description("Whether to skip the first line of each file. Supported formats are 'text', 'csv', 'tsv', " +
                   "'delimited'. Default value is false.")
    private Boolean skipHeader;

    // this is a hidden property that only exists for wrangler's parse-as-csv that uses the header as the schema
    // when this is true and the format is text, the header will be the first record returned by every record reader
    @Nullable
    private Boolean copyHeader;

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

    public GCSSourceConfig() {
      this.maxSplitSize = 128L * 1024 * 1024;
      this.recursive = false;
      this.filenameOnly = false;
      this.copyHeader = false;
    }

    public void validate(FailureCollector collector) {
      super.validate(collector);
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
      if (!containsMacro(NAME_FORMAT)) {
        try {
          getFormat();
        } catch (IllegalArgumentException e) {
          collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_FORMAT)
            .withStacktrace(e.getStackTrace());
        }
      }
    }

    @Override
    public String getReferenceName() {
      return referenceName;
    }

    @Override
    public String getPath() {
      return path;
    }

    @Override
    public FileFormat getFormat() {
      return FileFormat.from(format, FileFormat::canRead);
    }

    @Nullable
    @Override
    public Pattern getFilePattern() {
      try {
        return fileRegex == null ? null : Pattern.compile(fileRegex);
      } catch (RuntimeException e) {
        throw new IllegalArgumentException("Invalid file regular expression." + e.getMessage(), e);
      }
    }

    @Nullable
    public Pattern getExclusionPattern() {
      if (!isEncrypted()) {
        return null;
      }

      return Pattern.compile(".*" + Pattern.quote(getEncryptedMetadataSuffix()) + "$");
    }

    @Override
    public long getMaxSplitSize() {
      return maxSplitSize;
    }

    @Override
    public boolean shouldAllowEmptyInput() {
      return false;
    }

    @Override
    public boolean shouldReadRecursively() {
      return recursive;
    }

    @Nullable
    @Override
    public String getPathField() {
      return pathField;
    }

    @Override
    public boolean useFilenameAsPath() {
      return filenameOnly;
    }

    @Nullable
    @Override
    public Schema getSchema() {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to parse schema with error: " + e.getMessage(), e);
      }
    }

    public boolean isCopyHeader() {
      return copyHeader != null && copyHeader;
    }

    @Override
    public boolean skipHeader() {
      return skipHeader == null ? false : skipHeader;
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
