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
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.gcp.common.GCPConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * A action plugin to create directories within a given GCS bucket.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(GCSBucketCreate.NAME)
@Description("Creates objects in a Google Cloud Storage bucket.")
public final class GCSBucketCreate extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(GCSBucketCreate.class);
  public static final String NAME = "GCSBucketCreate";
  private Config config;

  @Override
  public void run(ActionContext context) throws Exception {
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

    configuration.set("fs.gs.system.bucket", config.bucket);
    configuration.setBoolean("fs.gs.impl.disable.cache", true);
    configuration.setBoolean("fs.gs.metadata.cache.enable", false);

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

    List<Path> undo = new ArrayList<>();
    boolean rollback = false;
    try {
      for (Path gcsPath : gcsPaths) {
        try {
          fs = gcsPaths.get(0).getFileSystem(configuration);
        } catch (IOException e) {
          rollback = true;
          throw new Exception("Unable to get GCS filesystem handler. " + e.getMessage(), e);
        }
        if (!fs.exists(gcsPath)) {
          try {
            fs.mkdirs(gcsPath);
            undo.add(gcsPath);
            LOG.info(String.format("Created GCS directory '%s''", gcsPath.toUri().getPath()));
          } catch (IOException e) {
            LOG.warn(
              String.format("Failed to create path '%s'", gcsPath)
            );
            rollback = true;
            throw e;
          }
        } else {
          if (config.failIfExists.equalsIgnoreCase("yes")) {
            rollback = true;
            throw new Exception(
              String.format("Object %s already exists")
            );
          }
        }
      }
    } finally {
      if (rollback) {
        context.getMetrics().gauge("gc.file.create.error", 1);
        for (Path path : undo) {
          try {
            fs = gcsPaths.get(0).getFileSystem(configuration);
            fs.delete(path, true);
          } catch (IOException e) {
            // No-op
          }
        }
      } else {
        context.getMetrics().gauge("gc.file.create.count", gcsPaths.size());
      }
    }
  }

  /**
   * Config for the plugin.
   */
  public final class Config extends GCPConfig {

    @Name("bucket")
    @Description("Name of the bucket.")
    @Macro
    public String bucket;

    @Name("paths")
    @Description("List of objects to be created within a bucket")
    @Macro
    public String paths;

    @Name("failIfExists")
    @Description("Fail if path exists.")
    @Macro
    public String failIfExists;
  }
}
