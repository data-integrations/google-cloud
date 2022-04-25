/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs.actions;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
  }

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate(context.getFailureCollector());

    Configuration configuration = new Configuration();

    Boolean isServiceAccountFilePath = config.isServiceAccountFilePath();
    if (isServiceAccountFilePath == null) {
      context.getFailureCollector().addFailure("Service account type is undefined.",
                                               "Must be `File Path` or `JSON`");
      context.getFailureCollector().getOrThrowException();
      return;
    }
    String serviceAccount = config.getServiceAccount();
    Credentials credentials = serviceAccount == null ?
      null : GCPUtils.loadServiceAccountCredentials(serviceAccount, isServiceAccountFilePath);
    if (serviceAccount != null) {
      Map<String, String> map = GCPUtils.generateGCSAuthProperties(serviceAccount, config.getServiceAccountType());
      map.forEach(configuration::set);
    }
    configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configuration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    // validate project id availability
    String projectId = config.getProject();
    configuration.set("fs.gs.project.id", projectId);
    configuration.set("fs.gs.path.encoding", "uri-path");

    configuration.setBoolean("fs.gs.impl.disable.cache", true);

    List<Path> gcsPaths = new ArrayList<>();
    List<GCSPath> gcsPathsWild = new ArrayList<>();
    Storage storage = GCPUtils.getStorage(config.getProject(), credentials);
    for (String path : config.getPaths()) {
      GCSPath gcsPath = GCSPath.from(path);
      // Check if the bucket is accessible
      try {
        storage.get(gcsPath.getBucket());
      } catch (StorageException e) {
        // Add more descriptive error message
        throw new RuntimeException(
          String.format("Unable to access or create bucket %s. ", gcsPath.getBucket())
            + "Ensure you entered the correct bucket path and have permissions for it.", e);
      }
      if (gcsPath.getUri().toString().contains("*")) {
        gcsPathsWild.add(gcsPath);
      } else {
        gcsPaths.add(new Path(gcsPath.getUri()));
      }
    }

    FileSystem fs;
    int deleteCount = 0;
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
          deleteCount++;
        } catch (IOException e) {
          LOG.warn(
            String.format("Failed to delete path '%s'", gcsPath)
          );
        }
      }
    }

    for (GCSPath gcsPath : gcsPathsWild) {
      String regex = ("\\Q" + gcsPath.getUri().toString() + "\\E").replace("*", "\\E[^/]*\\Q") + "(/.*)?";
      try {
        fs = new Path(gcsPathsWild.get(0).getUri()).getFileSystem(configuration);
      } catch (IOException e) {
        LOG.info("Failed deleting file " + gcsPath.getUri().getPath() + ", " + e.getMessage());
        // no-op.
        continue;
      }
      Page<Blob> blobs = storage.list(gcsPath.getBucket());
      for (Blob blob : blobs.iterateAll()) {
        Path filePath = new Path("gs://" + blob.getBucket() + "/" + blob.getName());
        if ((filePath.toString() + "/").matches(regex)) {
          if (fs.exists(filePath)) {
            // get root paths which also match regex to reduce delete times.
            Path rootPath = getRootMatch(filePath, regex);
            try {
              fs.delete(rootPath, true);
              deleteCount++;
            } catch (IOException e) {
              LOG.warn(
                String.format("Failed to delete path '%s'", gcsPath)
              );
            }
          } else {
            deleteCount++;
          }
        }
      }
    }
    context.getMetrics().gauge("gc.file.delete.count", deleteCount);
  }

  public Path getRootMatch(Path filePath, String regx) {
    Path current = filePath;
    while (current.getParent() != null && (current.getParent().toString() + "/").matches(regx)) {
      current = current.getParent();
    }
    return current;
  }

  /**
   * Config for the plugin.
   */
  public static final class Config extends GCPConfig {
    public static final String NAME_PATHS = "paths";

    @Name(NAME_PATHS)
    @Description("Comma separated list of objects to be deleted.")
    @Macro
    private String paths;

    public List<String> getPaths() {
      return Arrays.stream(paths.split(",")).map(String::trim).collect(Collectors.toList());
    }

    void validate(FailureCollector collector) {
      if (!containsMacro(NAME_PATHS)) {
        for (String path : getPaths()) {
          try {
            GCSPath.from(path);
          } catch (IllegalArgumentException e) {
            collector.addFailure(e.getMessage(), null).withConfigElement(NAME_PATHS, path);
          }
        }
        collector.getOrThrowException();
      }
    }
  }
}
