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

package co.cask.gcs;

import com.google.cloud.ServiceOptions;

import javax.annotation.Nullable;

/**
 * GCP Utility class to validate project id
 */
public class GCPUtil {

  /**
   * If project id is not provided and cannot be detected from environment, throw an exception indicating that else
   * return the project id.
   */
  public static String getProjectId(@Nullable String project) {
    String projectId = project == null ? ServiceOptions.getDefaultProjectId() : project;
    if (projectId == null) {
      throw new IllegalArgumentException(
        "Could not detect Google Cloud project id from the environment. Please specify a project id.");
    }
    return projectId;
  }
}
