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

package co.cask.gcs.actions;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;


/**
 * This action plugin <code>GCSBucketDelete</code> provides a way to delete
 * directories within a given GCS bucket.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(GCSBucketDelete.NAME)
@Description(GCSBucketDelete.DESCRIPTION)
public final class GCSBucketDelete extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(GCSBucketDelete.class);
  public static final String NAME = "GCSBucketDelete";
  public static final String DESCRIPTION = "Delete Google Cloud Storage Paths.";
  private Config config;

  @Override
  public void run(ActionContext context) throws Exception {
    Configuration configuration = new Configuration();
    configuration.set("google.cloud.auth.service.account.json.keyfile", config.serviceAccountFilePath);
    configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configuration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    configuration.set("fs.gs.project.id", config.project);
    configuration.set("fs.gs.system.bucket", config.bucket);
    configuration.setBoolean("fs.gs.impl.disable.cache", true);

    String[] paths = config.paths.split(",");
    List<Path> gcsPaths = new ArrayList<>();
    for (String path : paths) {
      String gcsPath = String.format("gs://%s/%s", config.bucket, path);
      if (path.startsWith("/")) {
        gcsPath = String.format("gs://%s%s", config.bucket, path);
      }
      gcsPaths.add(new Path(gcsPath));
    }

    FileSystem fs;
    int count = 0;
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
          if (config.variable != null) {
            context.getArguments().set(String.format("%s_%d", config.variable, count), gcsPath.toUri().toString());
          }
          count++;
        } catch (IOException e) {
          LOG.warn(
            String.format("Failed to delete path '%s'", gcsPath)
          );
        }
      }
    }
  }

  public final class Config extends PluginConfig {
    @Name("project")
    @Description("Project ID")
    @Macro
    public String project;

    @Name("serviceFilePath")
    @Description("Service account file path.")
    @Macro
    public String serviceAccountFilePath;

    @Name("bucket")
    @Description("Name of the bucket.")
    @Macro
    public String bucket;

    @Name("paths")
    @Description("List of paths to be deleted within a bucket")
    @Macro
    public String paths;

    @Name("variable")
    @Description("Token variable name used as base to pass files created to rest of pipeline. They are " +
      "available as ${variable_1}, ${variable_2} ... along with count in ${variable_count}")
    @Macro
    @Nullable
    public String variable;
  }
}
