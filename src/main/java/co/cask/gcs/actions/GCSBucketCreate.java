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
 * A action plugin to create directories within a given GCS bucket.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(GCSBucketCreate.NAME)
@Description(GCSBucketCreate.DESCRIPTION)
public final class GCSBucketCreate extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(GCSBucketCreate.class);
  public static final String NAME = "GCSBucketCreate";
  public static final String DESCRIPTION = "Creates Google Cloud Storage Bucket.";
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
              String.format("Path %s already exists")
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
        if (config.variable != null) {
          int count = 0;
          context.getArguments().set(String.format("%s_count", config.variable), String.valueOf(gcsPaths.size()));
          for (Path path : gcsPaths) {
            context.getArguments().set(String.format("%s_%s", config.variable, count), path.toUri().getPath());
            count++;
          }
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
    @Description("List of paths to be created within a bucket")
    @Macro
    public String paths;

    @Name("failIfExists")
    @Description("Fail if path exists.")
    @Macro
    public String failIfExists;

    @Name("variable")
    @Description("Token variable name used as base to pass files created to rest of pipeline. They are " +
      "available as ${variable_1}, ${variable_2} ... along with count in ${variable_count}")
    @Macro
    @Nullable
    public String variable;
  }
}
