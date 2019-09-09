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

package io.cdap.plugin.gcp.gcs.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.plugin.AbstractFileSink;
import io.cdap.plugin.format.plugin.FileSinkProperties;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;

import java.io.IOException;
import java.text.SimpleDateFormat;
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
  private final GCSBatchSinkConfig config;

  public GCSBatchSink(GCSBatchSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSinkContext context) {
    return GCPUtils.getFileSystemProperties(config);
  }

  @Override
  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordWrite("Write", "Wrote to Google Cloud Storage.", outputFields);
  }

  /**
   * Sink configuration.
   */
  @SuppressWarnings("unused")
  public static class GCSBatchSinkConfig extends GCPReferenceSinkConfig implements FileSinkProperties {
    private static final String SCHEME = "gs://";
    @Name("path")
    @Description("The path to write to. For example, gs://<bucket>/path/to/directory")
    @Macro
    private String path;

    @Description("The time format for the output directory that will be appended to the path. " +
      "For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. " +
      "If not specified, nothing will be appended to the path.")
    @Nullable
    @Macro
    private String suffix;

    @Description("The format to write in. The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', "
      + "or 'delimited'.")
    @Macro
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

    @Override
    public void validate() {
      // TODO: CDAP-15900 add failure collector
      super.validate(null);
      // validate that path is valid
      if (!containsMacro("path")) {
        GCSPath.from(path);
      }
      if (suffix != null && !containsMacro("suffix")) {
        new SimpleDateFormat(suffix);
      }
      if (!containsMacro("format")) {
        getFormat();
      }
      getSchema();
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
        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
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
  }
}
