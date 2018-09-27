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
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldReadOperation;
import co.cask.gcp.common.CombinePathTrackingInputFormat;
import co.cask.gcp.common.GCPReferenceSourceConfig;
import co.cask.gcp.common.PathTrackingInputFormat;
import co.cask.gcp.gcs.GCSConfigHelper;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class description here.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(GCSSource.NAME)
@Description("Reads objects from a path in a Google Cloud Storage bucket.")
public class GCSSource extends BatchSource<Object, Object, StructuredRecord> {
  public static final String NAME = "GCSFile";
  public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );
  private final GCSSourceConfig config;

  public GCSSource(GCSSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    // validate configs
    config.validate();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_SCHEMA);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    // validate configs
    config.validate();

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    Map<String, String> properties = new HashMap<>(config.getFileSystemProperties());
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      properties.put("google.cloud.auth.service.account.json.keyfile", serviceAccountFilePath);
    }
    properties.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    properties.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    String projectId = config.getProject();
    properties.put("fs.gs.project.id", projectId);
    properties.put("fs.gs.system.bucket", GCSConfigHelper.getBucket(config.path));
    properties.put("fs.gs.impl.disable.cache", "true");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    FileInputFormat.setInputDirRecursive(job, config.isRecursive());
    FileInputFormat.addInputPath(job, new Path(GCSConfigHelper.getPath(config.path)));

    if (config.maxSplitSize != null) {
      FileInputFormat.setMaxInputSplitSize(job, config.maxSplitSize * 1024 * 1024);
    }
    PathTrackingInputFormat.configure(conf, config.pathField, config.useFilenameOnly());
    context.setInput(Input.of(config.referenceName,
                              new SourceInputFormatProvider(CombinePathTrackingInputFormat.class, conf)));

    // record field level lineage information
    Schema outputSchema = context.getOutputSchema();
    if (outputSchema != null && outputSchema.getFields() != null && !outputSchema.getFields().isEmpty()) {
      FieldOperation operation = new FieldReadOperation("Read", "Read from Google Cloud Storage.",
                                                        EndPoint.of(context.getNamespace(), config.referenceName),
                                                        outputSchema.getFields().stream().map(Schema.Field::getName)
                                                          .collect(Collectors.toList()));
      context.record(Collections.singletonList(operation));
    }
  }

  /**
   * Config for the plugin.
   */
  public static class GCSSourceConfig extends GCPReferenceSourceConfig {
    private static final Gson GSON = new Gson();
    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    @Description("The path to read from. For example, gs://<bucket>/path/to/directory/")
    @Macro
    private String path;

    @Nullable
    @Description("Map of properties to set on the InputFormat.")
    @Macro
    private String fileSystemProperties;

    @Nullable
    @Description("Maximum split-size for each mapper in the MapReduce " +
      "Job. Defaults to 128MB.")
    @Macro
    private Long maxSplitSize;

    @Nullable
    @Description("If specified, each output record will include a field with this name that contains the file URI " +
      "that the record was read from. Requires a customized version of CombineFileInputFormat, so it cannot be used " +
      "if an inputFormatClass is given.")
    private String pathField;

    @Nullable
    @Description("Output schema. If a Path Field is set, it must be present in the schema as a string.")
    private String schema;

    @Nullable
    @Description("If true and a pathField is specified, only the filename will be used. If false, the full " +
      "URI will be used. Defaults to false.")
    private Boolean filenameOnly;

    @Nullable
    @Description("Boolean value to determine if files are to be read recursively from the path. Default is false.")
    private Boolean recursive;

    public GCSSourceConfig() {
      this.maxSplitSize = 128L;
      this.recursive = false;
      this.filenameOnly = false;
    }

    boolean useFilenameOnly() {
      return Boolean.TRUE.equals(filenameOnly);
    }

    boolean isRecursive() {
      return Boolean.TRUE.equals(recursive);
    }

    void validate() {
      // validate that path is valid
      GCSConfigHelper.getPath(path);
      getFileSystemProperties();
    }

    Map<String, String> getFileSystemProperties() {
      if (fileSystemProperties == null) {
        return Collections.emptyMap();
      }
      return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
  }
}
