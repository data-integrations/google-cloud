/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.spanner.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.spanner.common.SpannerConfig;

import javax.annotation.Nullable;

/**
 * Spanner source config
 */
public class SpannerSourceConfig extends SpannerConfig {

  @Description("Maximum number of partitions. This is only a hint. The actual number of partitions may vary")
  @Macro
  @Nullable
  public Long maxPartitions;

  @Description("Partition size in megabytes. This is only a hint. The actual partition size may vary")
  @Macro
  @Nullable
  public Long partitionSizeMB;

  @Description("Cloud Spanner table id. Uniquely identifies your table within the Cloud Spanner database")
  @Macro
  public String table;

  public SpannerSourceConfig(String referenceName) {
    super(referenceName);
  }

  @Override
  public void validate() {
    super.validate();
    if (!containsMacro("maxPartitions") && maxPartitions != null && maxPartitions < 1) {
      throw new IllegalArgumentException("Max partitions should be positive");
    }
    if (!containsMacro("partitionSizeMB") && partitionSizeMB != null && partitionSizeMB < 1) {
      throw new IllegalArgumentException("Partition size in mega bytes should be positive");
    }
  }
}
