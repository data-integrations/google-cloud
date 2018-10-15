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

package co.cask.gcp.gcs.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.gcp.common.GCPConfig;
import co.cask.gcp.common.GCPUtils;
import co.cask.gcp.common.ReferenceConfig;
import co.cask.gcp.common.ReferenceSink;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
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
  private final GCSBatchSinkConfig config;

  public GCSBatchSink(GCSBatchSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    config.validate();
    Map<String, String> outputConfig = new HashMap<>();
    outputConfig.put(FileOutputFormat.OUTDIR, config.getOutputDir(context.getLogicalStartTime()));
    if (config.serviceFilePath != null) {
      outputConfig.put("mapred.bq.auth.service.account.json.keyfile", config.serviceFilePath);
      outputConfig.put("google.cloud.auth.service.account.json.keyfile", config.serviceFilePath);
    }
    outputConfig.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    outputConfig.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    String projectId = GCPUtils.getProjectId(config.project);
    outputConfig.put("fs.gs.project.id", projectId);
    outputConfig.put("fs.gs.system.bucket", config.bucket);
    outputConfig.put("fs.gs.impl.disable.cache", "true");
    outputConfig.putAll(getOutputFormatConfig());

    context.addOutput(Output.of(config.referenceName,
                                new SinkOutputFormatProvider(getOutputFormatClassname(), outputConfig)));
  }

  protected abstract String getOutputFormatClassname();

  protected abstract Map<String, String> getOutputFormatConfig();

  @VisibleForTesting
  GCSBatchSinkConfig getConfig() {
    return config;
  }

  /**
   * Sink configuration.
   */
  public static class GCSBatchSinkConfig extends ReferenceConfig {
    @Name("path")
    @Description("The path to write to. For example, gs://<bucket>/path/to/directory")
    @Macro
    protected String path;

    @Description("The time format for the output directory that will be appended to the path. " +
      "For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. " +
      "If not specified, nothing will be appended to the path.")
    @Nullable
    @Macro
    protected String suffix;

    @Description(GCPConfig.PROJECT_DESC)
    @Macro
    @Nullable
    protected String project;

    @Description(GCPConfig.SERVICE_ACCOUNT_DESC)
    @Macro
    @Nullable
    protected String serviceFilePath;

    @Description("Name of the bucket.")
    @Macro
    protected String bucket;

    public GCSBatchSinkConfig() {
      // Set default value for Nullable properties.
      super("");
    }

    public void validate() {
      if (path != null && !containsMacro("path") && !path.startsWith("gs://")) {
        throw new IllegalArgumentException("Path must start with gs://.");
      }
      if (suffix != null && !containsMacro("suffix")) {
        new SimpleDateFormat(suffix);
      }
    }

    protected String getOutputDir(long logicalStartTime) {
      String timeSuffix = !Strings.isNullOrEmpty(suffix) ? new SimpleDateFormat(suffix).format(logicalStartTime) : "";
      return String.format("%s/%s", path, timeSuffix);
    }
  }
}
