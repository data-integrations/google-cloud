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
 *
 */

package io.cdap.plugin.gcp.gcs.connector;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;

import javax.annotation.Nullable;

/**
 * GCS connector config to include a hidden root bucket field
 */
public class GCSConnectorConfig extends GCPConnectorConfig {
  @Nullable
  @Description("The root bucket to browse when a root path is given")
  protected String rootBucket;

  public GCSConnectorConfig(@Nullable String project, @Nullable String serviceAccountType,
                            @Nullable String serviceFilePath, @Nullable String serviceAccountJson,
                            @Nullable String rootBucket) {
    super(project, serviceAccountType, serviceFilePath, serviceAccountJson);
    this.rootBucket = rootBucket;
  }
}
