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

package co.cask.gcp.gcs.actions;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.gcp.common.GCPConfig;
import co.cask.gcp.gcs.GCSPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


/**
 * This action plugin <code>GCSBucketDelete</code> provides a way to delete directories within a given GCS bucket.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(GCSBucketDelete.NAME)
@Description("Deletes objects from a Google Cloud Storage bucket")
public final class GCSBucketDelete extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(GCSBucketDelete.class);
  public static final String NAME = "GCSBucketDelete";
  private Config config;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
  }

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate();
    Configuration configuration = new Configuration();
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      configuration.set("google.cloud.auth.service.account.json.keyfile", serviceAccountFilePath);
    }
    configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configuration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    // validate project id availability
    String projectId = config.getProject();
    configuration.set("fs.gs.project.id", projectId);

    configuration.setBoolean("fs.gs.impl.disable.cache", true);

    List<Path> gcsPaths = new ArrayList<>();
    for (String path : config.getPaths()) {
      gcsPaths.add(new Path(GCSPath.from(path).getUri()));
    }

    FileSystem fs;
    context.getMetrics().gauge("gc.file.delete.count", gcsPaths.size());
    for (Path gcsPath : gcsPaths) {
      try {
        fs = gcsPaths.get(0).getFileSystem(configuration);
      } catch (IOException e) {
        LOG.info("Failed deleting file " + gcsPath.toUri().getPath() + ", " + e.getMessage());
        // no-op.
        continue;
      }
      if (fs.exists(gcsPath)) {
        try {
          fs.delete(gcsPath, true);
        } catch (IOException e) {
          LOG.warn(
            String.format("Failed to delete path '%s'", gcsPath)
          );
        }
      }
    }
  }

  /**
   * Config for the plugin.
   */
  public final class Config extends GCPConfig {
    @Name("paths")
    @Description("Comma separated list of objects to be deleted.")
    @Macro
    private String paths;

    public List<String> getPaths() {
      return Arrays.stream(paths.split(",")).map(String::trim).collect(Collectors.toList());
    }

    void validate() {
      if (containsMacro("paths")) {
        return;
      }
      for (String path : config.getPaths()) {
        GCSPath.from(path);
      }
    }
  }
}
