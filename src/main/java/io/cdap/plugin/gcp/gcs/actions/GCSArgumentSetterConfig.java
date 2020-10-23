/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.gcs.GCSPath;

/** Holds configuration required for configuring {@link GCSArgumentSetter}. */
public final class GCSArgumentSetterConfig extends GCPConfig {

  public static final String NAME_PATH = "path";

  @Name(NAME_PATH)
  @Macro
  @Description("GCS Path to the file containing the arguments")
  private String path;

  public void validate(FailureCollector collector) {
    validateProperties(collector);

    if (canConnect()) {
      try {
        GCSArgumentSetter.getContent(this);
      } catch (Exception e) {
        collector.addFailure("Can not get content from GCP!", null);
      }
    }
    collector.getOrThrowException();
  }

  public void validateProperties(FailureCollector collector) {
    if (!containsMacro(NAME_PATH)) {
      try {
        getPath();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_PATH);
      }
    }

    if (isServiceAccountJson()
      && !containsMacro(NAME_SERVICE_ACCOUNT_JSON)
      && Strings.isNullOrEmpty(getServiceAccountJson())) {
      collector
        .addFailure("Required property 'Service Account JSON' has no value.", "")
        .withConfigProperty(NAME_SERVICE_ACCOUNT_JSON);
    }
  }

  private boolean canConnect() {
    boolean canConnect =
        !containsMacro(NAME_PATH)
            && !(containsMacro(NAME_PROJECT) || AUTO_DETECT.equals(project))
            && !(containsMacro(NAME_SERVICE_ACCOUNT_TYPE));

    if (!canConnect) {
      return false;
    }

    if (!isServiceAccountJson()) {
      return !containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH)
          && !Strings.isNullOrEmpty(getServiceAccountFilePath());
    }
    return !containsMacro(NAME_SERVICE_ACCOUNT_JSON)
        && !Strings.isNullOrEmpty(getServiceAccountJson());
  }

  public GCSPath getPath() {
    return GCSPath.from(path);
  }
}
