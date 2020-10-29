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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * GCP utility class to get service account credentials
 */
public class GCPUtils {
  public static final String CMEK_KEY = "gcp.cmek.key.name";
  public static final String FS_GS_PROJECT_ID = "fs.gs.project.id";
  public static final String CLOUD_JSON_KEYFILE_SUFFIX = "auth.service.account.json.keyfile";
  public static final String CLOUD_JSON_KEYFILE_PREFIX = "google.cloud";
  public static final String CLOUD_JSON_KEYFILE = String.format("%s.%s", CLOUD_JSON_KEYFILE_PREFIX,
                                                                CLOUD_JSON_KEYFILE_SUFFIX);
  public static final String CLOUD_ACCOUNT_EMAIL_SUFFIX = "auth.service.account.email";
  public static final String CLOUD_ACCOUNT_PRIVATE_KEY_ID_SUFFIX = "auth.service.account.private.key.id";
  public static final String CLOUD_ACCOUNT_KEY_SUFFIX = "auth.service.account.private.key";
  public static final String CLOUD_ACCOUNT_JSON_SUFFIX = "auth.service.account.json";
  public static final String PRIVATE_KEY_WRAP = "-----BEGIN PRIVATE KEY-----\\n%s\\n-----END PRIVATE KEY-----\\n";
  public static final String SERVICE_ACCOUNT_TYPE = "cdap.gcs.auth.service.account.type";
  public static final String SERVICE_ACCOUNT_TYPE_FILE_PATH = "filePath";
  public static final String SERVICE_ACCOUNT_TYPE_JSON = "JSON";

  public static ServiceAccountCredentials loadServiceAccountCredentials(String path) throws IOException {
    File credentialsPath = new File(path);
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    }
  }

  public static ServiceAccountCredentials loadServiceAccountCredentials(String content,
                                                                        boolean isServiceAccountFilePath)
    throws IOException {
    if (isServiceAccountFilePath) {
      return loadServiceAccountCredentials(content);
    }
    InputStream jsonInputStream = new ByteArrayInputStream(content.getBytes());
    return ServiceAccountCredentials.fromStream(jsonInputStream);
  }

  public static String extractPrivateKey(ServiceAccountCredentials credentials) {
    return String.format(PRIVATE_KEY_WRAP,
                         Base64.getEncoder().encodeToString(credentials.getPrivateKey().getEncoded()));
  }

  /**
   * @param serviceAccount file path or Json content
   * @param serviceAccountType type of service account can be filePath or json
   * @param keyPrefix list of prefixes for which additional properties will be set.
   *                  <br>for account type filePath:
   *                  <ul>
   *                  <li>prefix + auth.service.account.json</li>
   *                  </ul>
   *                  for account type json:
   *                  <ul>
   *                  <li>prefix + auth.service.account.email</li>
   *                  <li>prefix + auth.service.account.private.key.id</li>
   *                  <li>prefix + auth.service.account.private.key</li>
   *                  </ul>
   *
   * @return {@link Map<String,String>} properties genereated based on input params
   * @throws IOException
   */
  public static Map<String, String> generateAuthProperties(String serviceAccount,
                                                           String serviceAccountType,
                                                           String... keyPrefix) throws IOException {
    Map<String, String> properties = new HashMap<>();
    String privateKeyData = null;
    properties.put(SERVICE_ACCOUNT_TYPE, serviceAccountType);

    boolean isServiceAccountFilePath = SERVICE_ACCOUNT_TYPE_FILE_PATH.equals(serviceAccountType);

    for (String prefix : keyPrefix) {
      if (isServiceAccountFilePath && serviceAccount != null) {
        properties.put(String.format("%s.%s", prefix, CLOUD_JSON_KEYFILE_SUFFIX), serviceAccount);
        continue;
      }
      ServiceAccountCredentials credentials = loadServiceAccountCredentials(serviceAccount, false);

      properties.put(String.format("%s.%s", prefix, CLOUD_ACCOUNT_EMAIL_SUFFIX), credentials.getClientEmail());
      properties.put(String.format("%s.%s", prefix, CLOUD_ACCOUNT_PRIVATE_KEY_ID_SUFFIX),
                     credentials.getPrivateKeyId());
      if (privateKeyData == null) {
        privateKeyData = extractPrivateKey(credentials);
      }
      properties.put(String.format("%s.%s", prefix, CLOUD_ACCOUNT_KEY_SUFFIX), privateKeyData);
      properties.put(String.format("%s.%s", prefix, CLOUD_ACCOUNT_JSON_SUFFIX), serviceAccount);
    }
    return properties;
  }

  public static Map<String, String> getFileSystemProperties(GCPConfig config, String path,
                                                            Map<String, String> properties) {
    try {
      properties.putAll(generateAuthProperties(config.getServiceAccount(), config.getServiceAccountType(),
                                               CLOUD_JSON_KEYFILE_PREFIX));
    } catch (Exception ignored) {

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
