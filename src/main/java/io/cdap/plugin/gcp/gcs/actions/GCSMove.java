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

import com.google.cloud.kms.v1.CryptoKeyName;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import io.cdap.plugin.gcp.gcs.StorageClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
  public void run(ActionContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector, context.getArguments().asMap());

    Boolean isServiceAccountFilePath = config.isServiceAccountFilePath();
    if (isServiceAccountFilePath == null) {
      collector.addFailure("Service account type is undefined.",
                           "Must be `filePath` or `JSON`");
      collector.getOrThrowException();
      return;
    }
    StorageClient storageClient = StorageClient.create(config.getProject(), config.getServiceAccount(),
                                                       isServiceAccountFilePath, config.readTimeout);
    GCSPath destPath = config.getDestPath();
    CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(config.cmekKey, context.getArguments().asMap(), collector);
    collector.getOrThrowException();

    // create the destination bucket if not exist
    storageClient.createBucketIfNotExists(destPath, config.location, cmekKeyName);

    List<GCSPath> matchedPaths = new ArrayList<>();
    if (SourceDestConfig.WILDCARD_REGEX.matcher(config.getSourcePath().getName()).find()) {
      matchedPaths = storageClient.getMatchedPaths(config.getSourcePath(), config.recursive,
          SourceDestConfig.WILDCARD_REGEX);
    } else {
      matchedPaths.add(config.getSourcePath());
    }
    for (GCSPath sourcePath : matchedPaths) {
      //noinspection ConstantConditions
      storageClient.move(sourcePath, config.getDestPath(), config.recursive, config.shouldOverwrite());
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
