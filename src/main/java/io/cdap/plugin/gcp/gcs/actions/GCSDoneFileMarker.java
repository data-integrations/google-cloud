/*
 * Copyright © 2021 Cask Data, Inc.
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
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.common.batch.action.Condition;
import io.cdap.plugin.common.batch.action.ConditionConfig;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

/**
 * A post action plugin that creates a marker file with a given name in case of a succeeded, failed or completed
 * pipeline.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(GCSDoneFileMarker.NAME)
@Description("Creates a marker file with a given name in case of a succeeded, failed or completed pipeline.")
public class GCSDoneFileMarker extends PostAction {
  private static final Logger LOG = LoggerFactory.getLogger(GCSDoneFileMarker.class);
  public static final String NAME = "GCSDoneFileMarker";
  public Config config;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
  }

  @Override
  public void run(BatchActionContext batchActionContext) throws IOException {
    FailureCollector collector = batchActionContext.getFailureCollector();
    config.validate(collector);

    Boolean isServiceAccountFilePath = config.isServiceAccountFilePath();
    if (isServiceAccountFilePath == null) {
      collector.addFailure("Service account type is undefined.", "Must be `filePath` or `JSON`.");
      collector.getOrThrowException();
      return;
    }

    if (!config.shouldRun(batchActionContext)) {
      LOG.debug("GCS done maker action is not run. No new marker file will be created.");
      return;
    }

    GCSPath markerFilePath = GCSPath.from(config.path);
    String serviceAccount = config.getServiceAccount();
    CryptoKeyName cmekKeyName = config.getCmekKey(batchActionContext.getArguments(),
                                                  batchActionContext.getFailureCollector());
    createFileMarker(config.getProject(), markerFilePath, serviceAccount, config.isServiceAccountFilePath(),
                     cmekKeyName);
  }

  /**
   * Config for the plugin.
   */
  public static class Config extends GCPConfig {
    public static final String NAME_PATH = "path";
    public static final String NAME_RUN_CONDITION = "runCondition";

    @Name(NAME_RUN_CONDITION)
    @Description("When to run the action. Must be 'completion', 'success', or 'failure'. Defaults to 'completion'. " +
      "If set to 'completion', the action will be executed and a marker file will get created regardless of whether " +
      "the pipeline run succeeded or failed. If set to 'success', the action will get executed and the marker file " +
      "will get created only if the pipeline run succeeded. If set to 'failure', the action will get executed and " +
      "the marker file will get created only if the pipeline run failed")
    public String runCondition;

    @Name(NAME_PATH)
    @Description("GCS path where the marker file will get created.")
    @Macro
    public String path;

    @Name(NAME_CMEK_KEY)
    @Macro
    @Nullable
    @Description("The GCP customer managed encryption key (CMEK) name used to encrypt data written to " +
      "any bucket created by the plugin. If the bucket already exists, this is ignored.")
    protected String cmekKey;

    Config() {
      super();
      this.runCondition = Condition.SUCCESS.name();
    }

    private Config(String project, String serviceAccountType, @Nullable String serviceFilePath,
                  @Nullable String serviceAccountJson, String gcsPath, @Nullable String cmekKey,
                  String runCondition) {
      this.serviceAccountType = serviceAccountType;
      this.serviceAccountJson = serviceAccountJson;
      this.serviceFilePath = serviceFilePath;
      this.project = project;
      this.path = gcsPath;
      this.cmekKey = cmekKey;
      this.runCondition = runCondition;
    }

    void validate(FailureCollector collector) {
      if (!this.containsMacro(NAME_RUN_CONDITION)) {
        new ConditionConfig(runCondition).validate(collector);
      }

      if (!containsMacro(NAME_PATH)) {
        try {
          GCSPath.from(path);
        } catch (IllegalArgumentException e) {
          collector.addFailure(e.getMessage(), "Please provide a valid GCS path.")
            .withConfigProperty(NAME_PATH);
        }
      }

      Boolean isServiceAccountFilePath = isServiceAccountFilePath();
      if (isServiceAccountFilePath != null && isServiceAccountFilePath
        && !containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) && Strings.isNullOrEmpty(getServiceAccountFilePath())
        && !AUTO_DETECT.equals(serviceFilePath)) {
        collector.addFailure("Required property 'Service Account File Path' has no value.", "")
          .withConfigProperty(NAME_SERVICE_ACCOUNT_FILE_PATH);
      }

      Boolean isServiceAccountJson = isServiceAccountJson();
      if (isServiceAccountJson != null && isServiceAccountJson && !containsMacro(NAME_SERVICE_ACCOUNT_JSON)
        && Strings.isNullOrEmpty(serviceAccountJson)) {
        collector.addFailure("Required property 'Service Account JSON' has no value.", "")
          .withConfigProperty(NAME_SERVICE_ACCOUNT_JSON);
      }

      if (!containsMacro(NAME_CMEK_KEY) && !Strings.isNullOrEmpty(cmekKey)) {
        validateCmekKey(collector);
      }

      collector.getOrThrowException();
    }

    void validateCmekKey(FailureCollector collector) {
      CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(cmekKey, collector);

      //these fields are needed to check if bucket exists or not and for location validation
      if (cmekKeyName == null || containsMacro(NAME_PATH) || projectOrServiceAccountContainsMacro()) {
        return;
      }
      Storage storage = GCPUtils.getStorage(getProject(), getCredentials(collector));
      if (storage == null) {
        return;
      }
      CmekUtils.validateCmekKeyAndBucketLocation(storage, GCSPath.from(path), cmekKeyName, null, collector);
    }

    public boolean shouldRun(BatchActionContext actionContext) {
      return new ConditionConfig(runCondition).shouldRun(actionContext);
    }

    public static Builder builder() {
      return new Builder();
    }

    /**
     * GCS Done File Marker configuration builder.
     */
    public static class Builder {
      private String serviceAccountType;
      private String serviceFilePath;
      private String serviceAccountJson;
      private String project;
      private String gcsPath;
      private String cmekKey;
      private String runCondition;

      public Builder setProject(@Nullable String project) {
        this.project = project;
        return this;
      }

      public Builder setServiceAccountType(@Nullable String serviceAccountType) {
        this.serviceAccountType = serviceAccountType;
        return this;
      }

      public Builder setServiceFilePath(@Nullable String serviceFilePath) {
        this.serviceFilePath = serviceFilePath;
        return this;
      }

      public Builder setServiceAccountJson(@Nullable String serviceAccountJson) {
        this.serviceAccountJson = serviceAccountJson;
        return this;
      }

      public Builder setGcsPath(@Nullable String gcsPath) {
        this.gcsPath = gcsPath;
        return this;
      }

      public Builder setCmekKey(@Nullable String cmekKey) {
        this.cmekKey = cmekKey;
        return this;
      }

      public Builder setRunCondition(String runCondition) {
        this.runCondition = runCondition;
        return this;
      }

      public Config build() {
        return new Config(
          project,
          serviceAccountType,
          serviceFilePath,
          serviceAccountJson,
          gcsPath,
          cmekKey,
          runCondition
        );
      }
    }
  }

  /**
   * Creates a marker file in a given GCS path. If an identical marker file already exists in the specified path, no
   * other marker file with the same name will get created. If the given bucket does not exist, it will get created
   * automatically.
   *
   * @param project The project Id.
   * @param path The GCS path to the file marker.
   * @param serviceAccount The service account.
   * @param isServiceAccountFilePath True, if a path is provided to the service account json file. False otherwise.
   * @param cmekKeyName CMEK name used for this bucket.
   */
  private static void createFileMarker(String project, GCSPath path, String serviceAccount,
                                       Boolean isServiceAccountFilePath, CryptoKeyName cmekKeyName) {
    Credentials credentials = null;
    if (serviceAccount != null) {
      try {
        credentials = GCPUtils.loadServiceAccountCredentials(serviceAccount, isServiceAccountFilePath);
      } catch (IOException e) {
        throw new RuntimeException(String.format("Failed to load credentials from path %s: %s.", serviceAccount,
                                                 e.getMessage()), e);
      }
    }

    Storage storage = GCPUtils.getStorage(project, credentials);
    if (storage.get(path.getBucket()) == null) {
      try {
        GCPUtils.createBucket(storage, path.getBucket(), null, cmekKeyName);
      } catch (StorageException e) {
        throw new RuntimeException(String.format("Failed to create bucket %s: %s.", path.getBucket(),
                                                 e.getMessage()), e);
      }
    }

    BlobId markerFileId = BlobId.of(path.getBucket(), path.getName());
    BlobInfo markerFileInfo = BlobInfo.newBuilder(markerFileId).build();
    try {
      storage.create(markerFileInfo, "".getBytes(StandardCharsets.UTF_8));
    } catch (StorageException e) {
      throw new RuntimeException(String.format("Failed to create the marker file at %s: %s.", path.getUri(),
                                               e.getMessage()), e);
    }
  }
}
