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

package co.cask.gcp.gcs.actions;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.gcp.common.GCPConfig;
import co.cask.gcp.gcs.GCSPath;

import javax.annotation.Nullable;

/**
 * Contains common properties for copy/move.
 */
public class SourceDestConfig extends GCPConfig {
  @Macro
  @Description("Path to a source object or directory.")
  private String sourcePath;

  @Macro
  @Description("Path to the destination. The bucket must already exist.")
  private String destPath;

  @Macro
  @Nullable
  @Description("Whether to overwrite existing objects.")
  private Boolean overwrite;

  public SourceDestConfig() {
    overwrite = false;
  }

  GCSPath getSourcePath() {
    return GCSPath.from(sourcePath);
  }

  GCSPath getDestPath() {
    return GCSPath.from(destPath);
  }

  @Nullable
  Boolean shouldOverwrite() {
    return overwrite;
  }

  public void validate() {
    if (!containsMacro("sourcePath")) {
      getSourcePath();
    }
    if (!containsMacro("destPath")) {
      getDestPath();
    }
  }
}
