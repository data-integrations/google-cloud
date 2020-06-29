/*
 * Copyright © 2020 AdaptiveScale, Inc.
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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySource;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;
import io.cdap.plugin.gcp.gcs.GCSPath;

/**
 * Holds configuration required for configuring {@link BigQuerySource}.
 */
public final class GCSArgumentSetterConfig extends GCPReferenceSourceConfig {

  public static final String NAME_PATH = "path";

  @Name(NAME_PATH)
  @Macro
  @Description("GCS Path to the file containing the arguments")
  private String path;

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);

    validateProperties(collector);

    if (canConnect()) {
      try {
        GCSArgumentSetter.getContent(this);
      } catch (Exception e) {
        collector.addFailure("Can not get content from GCP!", null);
      }
    }
  }

  public void validateProperties(FailureCollector collector) {
    if (!containsMacro(NAME_PATH)) {
      try {
        getPath();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_PATH);
      }
    }
  }

  private boolean canConnect() {
    return !Strings.isNullOrEmpty(getServiceAccountFilePath())
        && !(containsMacro(NAME_PROJECT) || AUTO_DETECT.equals(project))
        && !containsMacro(NAME_PATH);
  }

  public GCSPath getPath() {
    return GCSPath.from(path);
  }
}
