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

package io.cdap.plugin.gcp.dataplex.sink.connector;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.collect.Lists;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * Dataplex Connector configuration properties
 */
public class DataplexConnectorConfig extends GCPConnectorConfig {

  public DataplexConnectorConfig(@Nullable String project, @Nullable String serviceAccountType,
                                 @Nullable String serviceFilePath, @Nullable String serviceAccountJson) {
    super(project, serviceAccountType, serviceFilePath, serviceAccountJson);
  }

  public boolean isServiceAccountFilePathAutoDetect() {
    return serviceFilePath != null && AUTO_DETECT.equalsIgnoreCase(serviceFilePath);
  }


  public GoogleCredentials getCredentials() throws IOException {
    GoogleCredentials credentials = null;
    //validate service account
    if (isServiceAccountJson() || getServiceAccountFilePath() != null) {
      credentials =
        GCPUtils.loadServiceAccountCredentials(getServiceAccount(), isServiceAccountFilePath())
          .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
    } else if (isServiceAccountFilePathAutoDetect()) {
      credentials = ServiceAccountCredentials.getApplicationDefault();
    }
    return credentials;
  }
}
