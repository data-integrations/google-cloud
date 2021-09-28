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
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
    createFileMarker(config.getProject(), markerFilePath, serviceAccount, config.isServiceAccountFilePath(),
                     batchActionContext);
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

    Config() {
      super();
      this.runCondition = Condition.SUCCESS.name();
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

      collector.getOrThrowException();
    }

    public boolean shouldRun(BatchActionContext actionContext) {
      return new ConditionConfig(runCondition).shouldRun(actionContext);
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
   * @param context {@link BatchActionContext}
   */
  private static void createFileMarker(String project, GCSPath path, String serviceAccount,
                                       Boolean isServiceAccountFilePath,
                                       BatchActionContext context) {

    String cmekKey = context.getArguments().get(GCPUtils.CMEK_KEY);
    CryptoKeyName cmekKeyName = null;
    if (!Strings.isNullOrEmpty(cmekKey)) {
      cmekKeyName = CryptoKeyName.parse(cmekKey);
    }
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
