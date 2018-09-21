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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldReadOperation;
import co.cask.cdap.etl.api.lineage.field.FieldWriteOperation;
import co.cask.gcp.common.GCPConfig;
import co.cask.gcp.common.GCPUtils;
import co.cask.gcp.spanner.source.SpannerInputFormat;
import co.cask.hydrator.common.SourceInputFormatProvider;
import com.google.cloud.ServiceOptions;

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
public class GCSSource extends AbstractFileBatchSource {
  public static final String NAME = "GCSFile";
  private final GCSSourceConfig config;

  public GCSSource(GCSSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    super.prepareRun(context);
    config.validate();
    GCPUtils.getProjectId(config.project);

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

  public static class GCSSourceConfig extends FileSourceConfig {
    @Description("The path to read from. For example, gs://<bucket>/path/to/directory/")
    @Macro
    public String path;

    @Description(GCPConfig.PROJECT_DESC)
    @Macro
    @Nullable
    public String project;

    @Description(GCPConfig.SERVICE_ACCOUNT_DESC)
    @Macro
    @Nullable
    public String serviceFilePath;

    @Description("Name of the bucket.")
    @Macro
    public String bucket;

    @Override
    protected void validate() {
      super.validate();
      if (!containsMacro("path") && (!path.startsWith("gs://"))) {
        throw new IllegalArgumentException("Path must start with gs:// for Google Cloud Storage (GCS).");
      }
    }

    @Override
    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = new HashMap<>(super.getFileSystemProperties());
      if (serviceFilePath != null) {
        properties.put("mapred.bq.auth.service.account.json.keyfile", serviceFilePath);
        properties.put("google.cloud.auth.service.account.json.keyfile", serviceFilePath);
      }
      properties.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
      properties.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
      String projectId = project == null ? ServiceOptions.getDefaultProjectId() : project;
      properties.put("fs.gs.project.id", projectId);
      properties.put("fs.gs.system.bucket", bucket);
      properties.put("fs.gs.impl.disable.cache", "true");
      return properties;
    }

    @Override
    protected String getPath() {
      return path;
    }
  }

}
