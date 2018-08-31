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

package co.cask.spanner.common;

import co.cask.common.GCPUtils;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

import java.io.IOException;

/**
 * Spanner utility classs to get spanner service
 */
public class SpannerUtil {
  /**
   * Construct and return the {@link Spanner} service for the provided credentials and projectId
   */
  public static Spanner getSpannerService(String serviceAccountFilePath, String projectId) throws IOException {
    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder();
    if (serviceAccountFilePath != null) {
      optionsBuilder.setCredentials(GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath));
    }
    optionsBuilder.setProjectId(projectId);
    return optionsBuilder.build().getService();
  }
}
