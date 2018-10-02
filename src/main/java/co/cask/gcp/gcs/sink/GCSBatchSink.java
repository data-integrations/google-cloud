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
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.gcp.common.GCPReferenceSinkConfig;
import co.cask.gcp.gcs.GCSConfigHelper;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.format.FileFormat;
import co.cask.hydrator.format.plugin.AbstractFileSink;
import co.cask.hydrator.format.plugin.FileSinkProperties;
import com.google.common.base.Strings;

import java.io.IOException;
import java.text.SimpleDateFormat;
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
  private final GCSBatchSinkConfig config;

  public GCSBatchSink(GCSBatchSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSinkContext context) {
    Map<String, String> properties = new HashMap<>();
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
    return properties;
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
    private String format;

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
      GCSConfigHelper.getPath(path);
      if (suffix != null && !containsMacro("suffix")) {
        new SimpleDateFormat(suffix);
      }
      if (!containsMacro("format")) {
        getFormat();
      }
      getSchema();
    }

    @Override
    public String getReferenceName() {
      return referenceName;
    }

    @Override
    public String getPath() {
      return GCSConfigHelper.getPath(path).toString();
    }

    @Override
    public FileFormat getFormat() {
      return FileFormat.from(format, FileFormat::canWrite);
    }

    @Nullable
    public Schema getSchema() {
      if (containsMacro("schema") || schema == null) {
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
  }
}
