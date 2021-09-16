/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import io.cdap.plugin.gcp.gcs.StorageClient;

import java.io.IOException;
import javax.annotation.Nullable;


/**
 * An action plugin to move GCS objects.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(GCSMove.NAME)
@Description("Moves objects in Google Cloud Storage.")
public class GCSMove extends Action {
  public static final String NAME = "GCSMove";
  private Config config;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
  }

  @Override
  public void run(ActionContext context) throws IOException {
    config.validate(context.getFailureCollector());

    Boolean isServiceAccountFilePath = config.isServiceAccountFilePath();
    if (isServiceAccountFilePath == null) {
      context.getFailureCollector().addFailure("Service account type is undefined.",
                                               "Must be `filePath` or `JSON`");
      context.getFailureCollector().getOrThrowException();
      return;
    }
    StorageClient storageClient = StorageClient.create(config.getProject(), config.getServiceAccount(),
                                                       isServiceAccountFilePath);

    GCSPath destPath = config.getDestPath();
    GCSPath undo = null;
    Storage storage = storageClient.storage;

    // create the destination bucket if not exist
    Bucket bucket = null;
    try {
      bucket = storage.get(destPath.getBucket());
    } catch (StorageException e) {
      throw new RuntimeException(
        String.format("Unable to access or create bucket %s. ", destPath.getBucket())
          + "Ensure you entered the correct bucket path and have permissions for it.", e);
    }
    if (bucket == null) {
      GCPUtils.createBucket(storage, destPath.getBucket(), config.location,
                            config.getCmekKey(context.getArguments()));
      undo = destPath;
    }

    try {
      storageClient.move(config.getSourcePath(), config.getDestPath(), config.recursive, config.shouldOverwrite());
    } catch (Exception e) {
      //deleting the destination bucket that was auto-created if the operation failed.
      if (undo != null) {
        storage.delete(undo.getBucket());
      }
      throw e;
    }
  }

  /**
   * Config for the plugin.
   */
  public static class Config extends SourceDestConfig {

    @Macro
    @Nullable
    @Description("Whether to move objects in all subdirectories.")
    private Boolean recursive;

    public Config() {
      super();
      recursive = false;
    }
  }
}
