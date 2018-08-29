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

package co.cask.gcs.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.common.ReferenceConfig;
import co.cask.common.ReferenceSink;
import co.cask.gcs.GCPUtil;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link GCSBatchSink} that stores the data of the latest run of an adapter in S3.
 *
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class GCSBatchSink<KEY_OUT, VAL_OUT> extends ReferenceSink<StructuredRecord, KEY_OUT, VAL_OUT> {

  public static final String PATH_DESC = "The GCS path where the data is stored. Example: 'gs://logs'.";
  private static final String PATH_FORMAT_DESCRIPTION = "The format for the path that will be suffixed to the " +
    "base path; for example: the format 'yyyy-MM-dd-HH-mm' will create a file path ending in '2015-01-01-20-42'. " +
    "Default format used is 'yyyy-MM-dd-HH-mm'.";
  private static final String FILESYSTEM_PROPERTIES_DESCRIPTION = "A JSON string representing a map of properties " +
    "needed for the distributed file system.";
  private static final String DEFAULT_PATH_FORMAT = "yyyy-MM-dd-HH-mm";
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() {
  }.getType();

  private final GCSBatchSinkConfig config;

  public GCSBatchSink(GCSBatchSinkConfig config) {
    super(config);
    this.config = config;
    // update properties to include accessID and accessKey, so prepareRun can only set properties
    // in configuration, and not deal with accessID and accessKey separately
    // do not create file system properties if macros were provided unless in a test case
    if (!this.config.containsMacro("properties") && !this.config.containsMacro("accessID") &&
      !this.config.containsMacro("accessKey")) {
      this.config.properties = updateFileSystemProperties(this.config.path,
                                                          this.config.properties);
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public final void prepareRun(BatchSinkContext context) {
    config.validate();
    // validate project id availability
    GCPUtil.getProjectId(config.project);
    OutputFormatProvider outputFormatProvider = createOutputFormatProvider(context);
    Map<String, String> outputConfig = new HashMap<>(outputFormatProvider.getOutputFormatConfiguration());

    if (config.properties != null) {
      Map<String, String> properties = GSON.fromJson(config.properties, MAP_STRING_STRING_TYPE);
      outputConfig.putAll(properties);
    }
    context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider(
      outputFormatProvider.getOutputFormatClassName(), outputConfig)));
  }

  /**
   * Returns a {@link OutputFormatProvider} to be used for output.
   */
  protected abstract OutputFormatProvider createOutputFormatProvider(BatchSinkContext context);

  private static String updateFileSystemProperties(String basePath, @Nullable String fileSystemProperties) {
    Map<String, String> providedProperties;
    if (fileSystemProperties == null) {
      providedProperties = new HashMap<>();
    } else {
      providedProperties = GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
    return GSON.toJson(providedProperties);
  }

  @VisibleForTesting
  GCSBatchSinkConfig getConfig() {
    return config;
  }

  /**
   * S3 Sink configuration.
   */
  public static class GCSBatchSinkConfig extends ReferenceConfig {
    @Name("path")
    @Description("The GCS path where the data is stored. Example: 'gs://logs'.")
    @Macro
    protected String path;

    @Name("format")
    @Description("The format for the path that will be suffixed to the " +
      "base path; for example: the format 'yyyy-MM-dd-HH-mm' will create a file path ending in '2015-01-01-20-42'. " +
      "Default format used is 'yyyy-MM-dd-HH-mm'.")
    @Nullable
    @Macro
    protected String format;

    @Name("properties")
    @Description("A JSON string representing a map of properties " +
      "needed for the distributed file system.")
    @Nullable
    @Macro
    protected String properties;

    @Name("project")
    @Description("Project ID")
    @Macro
    @Nullable
    protected String project;

    @Name("serviceFilePath")
    @Description("Service account file path.")
    @Macro
    @Nullable
    protected String serviceAccountFilePath;

    @Name("bucket")
    @Description("Name of the bucket.")
    @Macro
    protected String bucket;

    public GCSBatchSinkConfig() {
      // Set default value for Nullable properties.
      super("");
      this.format = DEFAULT_PATH_FORMAT;
      this.properties = updateFileSystemProperties(path, null);
    }

    public GCSBatchSinkConfig(String referenceName, String path,
                              @Nullable String format, @Nullable String properties,
                              String project, String serviceAccountFilePath, String bucket) {
      super(referenceName);
      this.path = path;
      this.format = format == null || format.isEmpty() ? DEFAULT_PATH_FORMAT : format;
      this.project = project;
      this.serviceAccountFilePath = serviceAccountFilePath;
      this.bucket = bucket;
      this.properties = updateFileSystemProperties(path, properties);
    }

    public void validate() {
      if (path != null && !path.startsWith("gs://")) {
        throw new IllegalArgumentException("Path must start with gs://.");
      }
    }
  }
}
