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

package co.cask.gcp.common;

import co.cask.gcp.gcs.GCSConfigHelper;
import co.cask.gcp.gcs.sink.GCSBatchSink.GCSBatchSinkConfig;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQueryOptions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * GCP utility class to get service account credentials
 */
public class GCPUtils {

  public static ServiceAccountCredentials loadCredentials(String path) throws IOException {
    File credentialsPath = new File(path);
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    }
  }

  public static Map<String, String> getFileSystemProperties(GCSBatchSinkConfig config) {
    Map<String, String> properties = new HashMap<>();
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      properties.put("google.cloud.auth.service.account.json.keyfile", serviceAccountFilePath);
    }
    properties.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    properties.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    String projectId = config.getProject();
    properties.put("fs.gs.project.id", projectId);
    properties.put("fs.gs.system.bucket", GCSConfigHelper.getBucket(config.getPath()));
    properties.put("fs.gs.working.dir", GCSConfigHelper.ROOT_DIR);
    properties.put("fs.gs.impl.disable.cache", "true");
    return properties;
  }

//  /**
//   *
//   * @param projectId
//   * @param serviceFilePath
//   * @return
//   * @throws Exception
//   */
//  public static BigQueryOptions.Builder getBigQuery(String projectId, String serviceFilePath) throws Exception {
//    BigQueryOptions.Builder builder = BigQueryOptions.newBuilder();
//    if (serviceFilePath != null) {
//      builder.setCredentials(loadCredentials(serviceFilePath));
//    }
//    String project = projectId == null ? ServiceOptions.getDefaultProjectId() : projectId;
//    if (project == null) {
//      throw new Exception("Could not detect Google Cloud project id from the environment. " +
//                            "Please specify a project id.");
//    }
//    builder.setProjectId(project);
//    return builder.;
//  }
}
