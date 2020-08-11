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

package io.cdap.plugin.gcp.common;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.cdap.plugin.gcp.gcs.GCSPath;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * GCP utility class to get service account credentials
 */
public class GCPUtils {
  public static final String CMEK_KEY = "gcp.cmek.key.name";
  public static final String FS_GS_PROJECT_ID = "fs.gs.project.id";
  public static final String CLOUD_JSON_KEYFILE = "google.cloud.auth.service.account.json.keyfile";

  public static ServiceAccountCredentials loadServiceAccountCredentials(String path) throws IOException {
    File credentialsPath = new File(path);
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    }
  }

  public static Map<String, String> getFileSystemProperties(GCPConfig config, String path,
                                                            Map<String, String> properties) {
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      properties.put(CLOUD_JSON_KEYFILE, serviceAccountFilePath);
    }
    properties.put("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
    properties.put("fs.AbstractFileSystem.gs.impl", GoogleHadoopFS.class.getName());
    String projectId = config.getProject();
    properties.put(FS_GS_PROJECT_ID, projectId);
    properties.put("fs.gs.system.bucket", GCSPath.from(path).getBucket());
    properties.put("fs.gs.path.encoding", "uri-path");
    properties.put("fs.gs.working.dir", GCSPath.ROOT_DIR);
    properties.put("fs.gs.impl.disable.cache", "true");
    return properties;
  }

  public static BigQuery getBigQuery(String project, @Nullable Credentials credentials) {
    BigQueryOptions.Builder bigqueryBuilder = BigQueryOptions.newBuilder().setProjectId(project);
    if (credentials != null) {
      bigqueryBuilder.setCredentials(credentials);
    }
    return bigqueryBuilder.build().getService();
  }

  public static Storage getStorage(String project, @Nullable Credentials credentials) {
    StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(project);
    if (credentials != null) {
      builder.setCredentials(credentials);
    }
    return builder.build().getService();
  }

  public static void createBucket(Storage storage, String bucket, @Nullable String location,
                                  @Nullable String cmekKey) throws StorageException {
    BucketInfo.Builder builder = BucketInfo.newBuilder(bucket);
    if (location != null) {
      builder.setLocation(location);
    }
    if (cmekKey != null) {
      builder.setDefaultKmsKeyName(cmekKey);
    }
    storage.create(builder.build());
  }
}
