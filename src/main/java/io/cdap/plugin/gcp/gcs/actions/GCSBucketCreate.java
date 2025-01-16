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
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPErrorDetailsProviderUtil;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector, context.getArguments().asMap());

    Configuration configuration = new Configuration();

    Boolean isServiceAccountFilePath = config.isServiceAccountFilePath();
    if (isServiceAccountFilePath == null) {
      collector.addFailure("Service account type is undefined.",
                           "Must be `filePath` or `JSON`");
      collector.getOrThrowException();
      return;
    }
    String serviceAccount = config.getServiceAccount();
    Credentials credentials = null;
    try {
      credentials = serviceAccount == null ? null : GCPUtils.loadServiceAccountCredentials(serviceAccount,
        isServiceAccountFilePath);
    } catch (IOException e) {
      String errorReason = "Failed to load service account credentials.";
      collector.addFailure(String.format("%s %s: %s", errorReason, e.getClass().getName(), e.getMessage()), null)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }
    Map<String, String> map = GCPUtils.generateGCSAuthProperties(serviceAccount, config.getServiceAccountType());
    map.forEach(configuration::set);

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
      CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(config.cmekKey, context.getArguments().asMap(), collector);
      collector.getOrThrowException();
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
        } catch (Exception e) {
          // Add more descriptive error message
          String errorReason = String.format("Unable to access GCS bucket '%s'", gcsPath.getBucket());
          throw GCPErrorDetailsProviderUtil.getHttpResponseExceptionDetailsFromChain(e, errorReason, ErrorType.UNKNOWN,
            true, GCPUtils.GCS_SUPPORTED_DOC_URL);
        }
        if (bucket == null) {
          GCPUtils.createBucket(storage, gcsPath.getBucket(), config.location, cmekKeyName);
          undoBucket.add(bucketPath);
        } else if (gcsPath.equals(bucketPath) && config.failIfExists()) {
          // if the gcs path is just a bucket, and it exists, fail the pipeline
          rollback = true;
          String errorReason = String.format("Path %s already exists", gcsPath);
          throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
            errorReason, errorReason, ErrorType.USER, true, null);
        }
      }

      for (Path gcsPath : gcsPaths) {
        try {
          fs = gcsPath.getFileSystem(configuration);
        } catch (IOException e) {
          rollback = true;
          String errorReason = "Unable to get GCS filesystem handler.";
          throw GCPErrorDetailsProviderUtil.getHttpResponseExceptionDetailsFromChain(e, errorReason, ErrorType.UNKNOWN,
            true, GCPUtils.GCS_SUPPORTED_DOC_URL);
        }
        if (!fs.exists(gcsPath)) {
          try {
            fs.mkdirs(gcsPath);
            undo.add(gcsPath);
            LOG.info(String.format("Created GCS directory '%s''", gcsPath.toUri().getPath()));
          } catch (IOException e) {
            LOG.warn(String.format("Failed to create path '%s'", gcsPath));
            rollback = true;
            String errorReason = String.format("Failed to create path %s.", gcsPath);
            throw GCPErrorDetailsProviderUtil.getHttpResponseExceptionDetailsFromChain(e, errorReason,
              ErrorType.UNKNOWN, true, GCPUtils.GCS_SUPPORTED_DOC_URL);
          }
        } else {
          if (config.failIfExists()) {
            rollback = true;
            String errorReason = String.format("Path %s already exists", gcsPath);
            throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
              errorReason, errorReason, ErrorType.SYSTEM, true, null);
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

    @Name(NAME_CMEK_KEY)
    @Macro
    @Nullable
    @Description("The GCP customer managed encryption key (CMEK) name used to encrypt data written to " +
      "any bucket created by the plugin. If the bucket already exists, this is ignored. More information can be found" +
      " at https://cloud.google.com/data-fusion/docs/how-to/customer-managed-encryption-keys")
    private String cmekKey;

    public Config(@Nullable String project, @Nullable String serviceAccountType, @Nullable String serviceFilePath,
                  @Nullable String serviceAccountJson, @Nullable String paths, @Nullable String location,
                  @Nullable String cmekKey) {
      this.serviceAccountType = serviceAccountType;
      this.serviceAccountJson = serviceAccountJson;
      this.serviceFilePath = serviceFilePath;
      this.project = project;
      this.paths = paths;
      this.location = location;
      this.cmekKey = cmekKey;
    }

    public List<String> getPaths() {
      return Arrays.stream(paths.split(",")).map(String::trim).collect(Collectors.toList());
    }

    public boolean failIfExists() {
      return failIfExists;
    }
    void validate(FailureCollector collector) {
      validate(collector, Collections.emptyMap());
    }
    
    void validate(FailureCollector collector, Map<String, String> arguments) {
      if (!containsMacro("paths")) {
        for (String path : getPaths()) {
          try {
            GCSPath.from(path);
          } catch (IllegalArgumentException e) {
            collector.addFailure(e.getMessage(), null).withConfigElement(NAME_PATHS, path);
          }
        }
      }
      if (!containsMacro(NAME_CMEK_KEY)) {
        validateCmekKey(collector, arguments);
      }
      collector.getOrThrowException();
    }

    //This method validated the pattern of CMEK Key resource ID.
    void validateCmekKey(FailureCollector failureCollector, Map<String, String> arguments) {
      CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(cmekKey, arguments, failureCollector);

      //these fields are needed to check if bucket exists or not and for location validation
      if (cmekKeyName == null || containsMacro(NAME_PATHS) || containsMacro(NAME_LOCATION) ||
        projectOrServiceAccountContainsMacro()) {
        return;
      }
      Storage storage = GCPUtils.getStorage(getProject(), getCredentials(failureCollector));
      if (storage == null) {
        return;
      }
      for (String path : getPaths()) {
        CmekUtils.validateCmekKeyAndBucketLocation(storage, GCSPath.from(path),
                                                   cmekKeyName, location, failureCollector);
      }
    }

    public static Config.Builder builder() {
      return new Config.Builder();
    }

    /**
     * GCS Bucket Create configuration builder.
     */
    public static class Builder {
      private String serviceAccountType;
      private String serviceFilePath;
      private String serviceAccountJson;
      private String project;
      private String gcsPaths;
      private String cmekKey;
      private String location;

      public GCSBucketCreate.Config.Builder setProject(@Nullable String project) {
        this.project = project;
        return this;
      }

      public GCSBucketCreate.Config.Builder setServiceAccountType(@Nullable String serviceAccountType) {
        this.serviceAccountType = serviceAccountType;
        return this;
      }

      public GCSBucketCreate.Config.Builder setServiceFilePath(@Nullable String serviceFilePath) {
        this.serviceFilePath = serviceFilePath;
        return this;
      }

      public GCSBucketCreate.Config.Builder setServiceAccountJson(@Nullable String serviceAccountJson) {
        this.serviceAccountJson = serviceAccountJson;
        return this;
      }

      public GCSBucketCreate.Config.Builder setGcsPath(@Nullable String gcsPaths) {
        this.gcsPaths = gcsPaths;
        return this;
      }

      public GCSBucketCreate.Config.Builder setCmekKey(@Nullable String cmekKey) {
        this.cmekKey = cmekKey;
        return this;
      }

      public GCSBucketCreate.Config.Builder setLocation(@Nullable String location) {
        this.location = location;
        return this;
      }

      public GCSBucketCreate.Config build() {
        return new GCSBucketCreate.Config(
          project,
          serviceAccountType,
          serviceFilePath,
          serviceAccountJson,
          gcsPaths,
          location,
          cmekKey
        );
      }
    }
  }
}
