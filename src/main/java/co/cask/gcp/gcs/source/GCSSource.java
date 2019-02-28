/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.gcp.gcs.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.gcp.common.GCPReferenceSourceConfig;
import co.cask.gcp.gcs.GCSPath;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.format.FileFormat;
import co.cask.hydrator.format.input.PathTrackingInputFormat;
import co.cask.hydrator.format.plugin.AbstractFileSource;
import co.cask.hydrator.format.plugin.FileSourceProperties;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

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
    config.validate();
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSourceContext context) {
    Map<String, String> properties = new HashMap<>(config.getFileSystemProperties());
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      properties.put("google.cloud.auth.service.account.json.keyfile", serviceAccountFilePath);
    }
    properties.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    properties.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    String projectId = config.getProject();
    properties.put("fs.gs.project.id", projectId);
    properties.put("fs.gs.system.bucket", GCSPath.from(config.path).getBucket());
    properties.put("fs.gs.working.dir", GCSPath.ROOT_DIR);
    properties.put("fs.gs.impl.disable.cache", "true");
    if (config.copyHeader) {
      properties.put(PathTrackingInputFormat.COPY_HEADER, "true");
    }
    return properties;
  }

  @Override
  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordRead("Read", "Read from Google Cloud Storage.", outputFields);
  }

  /**
   * Config for the plugin.
   */
  @SuppressWarnings("ConstantConditions")
  public static class GCSSourceConfig extends GCPReferenceSourceConfig implements FileSourceProperties {
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

    @Nullable
    @Description("Output field to place the path of the file that the record was read from. "
      + "If not specified, the file path will not be included in output records. "
      + "If specified, the field must exist in the output schema as a string.")
    private String pathField;

    @Macro
    @Nullable
    @Description("Format of the data to read. Supported formats are 'avro', 'blob', 'csv', 'delimited', 'json', "
      + "'parquet', 'text', and 'tsv'.")
    private String format;

    @Macro
    @Nullable
    @Description("Output schema. If a Path Field is set, it must be present in the schema as a string.")
    private String schema;

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

    // this is a hidden property that only exists for wrangler's parse-as-csv that uses the header as the schema
    // when this is true and the format is text, the header will be the first record returned by every record reader
    @Nullable
    private Boolean copyHeader;

    public GCSSourceConfig() {
      this.maxSplitSize = 128L * 1024 * 1024;
      this.recursive = false;
      this.filenameOnly = false;
      this.copyHeader = false;
    }

    public void validate() {
      super.validate();
      // validate that path is valid
      if (!containsMacro("path")) {
        GCSPath.from(path);
      }
      getFileSystemProperties();
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
        throw new IllegalArgumentException("Invalid file regular expression: " + e.getMessage(), e);
      }
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
        return schema == null ? null : Schema.parseJson(schema);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to parse schema with error: " + e.getMessage(), e);
      }
    }

    Map<String, String> getFileSystemProperties() {
      if (fileSystemProperties == null) {
        return Collections.emptyMap();
      }
      return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
  }
}
