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

import com.google.auth.Credentials;
import com.google.cloud.storage.Bucket;
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
import java.util.stream.Collectors;
import javax.annotation.Nullable;


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
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
  }

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate(context.getFailureCollector());

    Configuration configuration = new Configuration();

    String serviceAccountFilePath = config.getServiceAccountFilePath();
    Credentials credentials = serviceAccountFilePath == null ?
                                null : GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath);

    if (serviceAccountFilePath != null) {
      configuration.set("google.cloud.auth.service.account.json.keyfile", serviceAccountFilePath);
    }
    configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configuration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    // validate project id availability
    String projectId = config.getProject();
    configuration.set("fs.gs.project.id", projectId);
    configuration.set("fs.gs.path.encoding", "uri-path");

    configuration.setBoolean("fs.gs.impl.disable.cache", true);
    configuration.setBoolean("fs.gs.metadata.cache.enable", false);

    FileSystem fs;

    List<Path> undo = new ArrayList<>();
    List<GCSPath> undoBucket = new ArrayList<>();
    List<Path> gcsPaths = new ArrayList<>();
    Storage storage = GCPUtils.getStorage(config.getProject(), credentials);
    boolean rollback = false;
    try {
      for (String path : config.getPaths()) {
        GCSPath gcsPath = GCSPath.from(path);
        GCSPath bucketPath = GCSPath.from(GCSPath.SCHEME + gcsPath.getBucket());

        // only add it to gcspaths if the path has directories after bucket
        if (!gcsPath.equals(bucketPath)) {
          gcsPaths.add(new Path(gcsPath.getUri()));
        }

        // create the gcs buckets if not exist
        Bucket bucket = null;
        try {
          bucket = storage.get(gcsPath.getBucket());
        } catch (StorageException e) {
          // Add more descriptive error message
          throw new RuntimeException(
            String.format("Unable to access or create bucket %s. ", gcsPath.getBucket())
              + "Ensure you entered the correct bucket path and have permissions for it.", e);
        }
        if (bucket == null) {
          GCPUtils.createBucket(storage, gcsPath.getBucket(), config.location,
                                context.getArguments().get(GCPUtils.CMEK_KEY));
          undoBucket.add(bucketPath);
        } else if (gcsPath.equals(bucketPath) && config.failIfExists()) {
          // if the gcs path is just a bucket, and it exists, fail the pipeline
          rollback = true;
          throw new Exception(String.format("Path %s already exists", gcsPath));
        }
      }

      for (Path gcsPath : gcsPaths) {
        try {
          fs = gcsPath.getFileSystem(configuration);
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
            LOG.warn(String.format("Failed to create path '%s'", gcsPath));
            rollback = true;
            throw e;
          }
        } else {
          if (config.failIfExists()) {
            rollback = true;
            throw new Exception(String.format("Path %s already exists", gcsPath));
          }
        }
      }
    } finally {
      if (rollback) {
        context.getMetrics().gauge("gc.file.create.error", 1);
        // this delete can only delete directories, bucket cannot be deleted this way
        for (Path path : undo) {
          try {
            fs = path.getFileSystem(configuration);
            fs.delete(path, true);
          } catch (IOException e) {
            // No-op
          }
        }

        // delete buckets
        for (GCSPath bucket : undoBucket) {
          storage.delete(bucket.getBucket());
        }
      } else {
        context.getMetrics().gauge("gc.file.create.count", gcsPaths.size());
      }
    }
  }

  /**
   * Config for the plugin.
   */
  public static final class Config extends GCPConfig {
    public static final String NAME_PATHS = "paths";
    public static final String NAME_LOCATION = "location";

    @Name(NAME_PATHS)
    @Description("Comma separated list of objects to be created.")
    @Macro
    private String paths;

    @Name("failIfExists")
    @Description("Fail if path exists.")
    @Macro
    private boolean failIfExists;

    @Name(NAME_LOCATION)
    @Macro
    @Nullable
    @Description("The location where the gcs buckets will get created. " +
                   "This value is ignored if the bucket already exists.")
    protected String location;

    public List<String> getPaths() {
      return Arrays.stream(paths.split(",")).map(String::trim).collect(Collectors.toList());
    }

    public boolean failIfExists() {
      return failIfExists;
    }

    void validate(FailureCollector collector) {
      if (!containsMacro("paths")) {
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
