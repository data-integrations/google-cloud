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
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.util.AccessTokenProviderClassFromConfigFactory;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.cdap.plugin.gcp.gcs.GCSPath;
import io.cdap.plugin.gcp.gcs.ServiceAccountAccessTokenProvider;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * GCP utility class to get service account credentials
 */
public class GCPUtils {
  public static final String FS_GS_PROJECT_ID = "fs.gs.project.id";
  private static final String SERVICE_ACCOUNT_TYPE = "cdap.gcs.auth.service.account.type";
  private static final String SERVICE_ACCOUNT = "cdap.gcs.auth.service.account";
  private static final String SERVICE_ACCOUNT_TYPE_FILE_PATH = "filePath";
  // according to https://cloud.google.com/bigquery/external-data-drive, to read from external table, drive scope
  // needs to be added, by default, this scope is not included
  public static final List<String> BIGQUERY_SCOPES = Arrays.asList("https://www.googleapis.com/auth/drive",
                                                                   "https://www.googleapis.com/auth/bigquery");

  /**
   * Load a service account from the local file system.
   *
   * @param path path to the service account file
   * @return credentials generated from the service account file
   * @throws IOException if there was an error reading the file
   */
  public static GoogleCredentials loadServiceAccountCredentials(String path) throws IOException {
    return loadServiceAccountCredentials(path, true);
  }

  /**
   * Load a service account from either a file on the local file system, or from the raw json content of the
   * service account
   *
   * @param serviceAccount either a path to the service account file or the raw contents of the service account
   * @param isServiceAccountFilePath whether the service account if a file path or not
   * @return credentials loaded from the file or account contents
   * @throws IOException if there was an error reading from the service account file
   */
  public static GoogleCredentials loadServiceAccountCredentials(String serviceAccount,
                                                                boolean isServiceAccountFilePath)
    throws IOException {
    try (InputStream inputStream = openServiceAccount(serviceAccount, isServiceAccountFilePath)) {
      return GoogleCredentials.fromStream(inputStream);
    }
  }

  /**
   * Open an InputStream to read the service account, whether it is the raw account contents or a path to a
   * local file.
   *
   * @param serviceAccount either a path to the service account file or the raw contents of the service account
   * @param isFile whether the service account if a file path or not
   * @return InputStream from which to read the service account
   * @throws IOException if there was an error reading from the service account file
   */
  public static InputStream openServiceAccount(String serviceAccount, boolean isFile) throws FileNotFoundException {
    if (isFile) {
      return new FileInputStream(serviceAccount);
    }
    return new ByteArrayInputStream(serviceAccount.getBytes(StandardCharsets.UTF_8));
  }


  /**
   * @param serviceAccount file path or Json content
   * @param serviceAccountType type of service account can be filePath or json
   *
   * @return {@link Map} properties genereated based on input params
   */
  public static Map<String, String> generateGCSAuthProperties(@Nullable String serviceAccount,
                                                              String serviceAccountType) {
    // fs.gs prefix is for GoogleHadoopFileSystemBase.getCredential(), used by the GCS connector.
    return generateAuthProperties(serviceAccount, serviceAccountType, "fs.gs");
  }

  /**
   * Generate auth related properties to set in the Hadoop configuration.
   *
   * @param serviceAccount file path or Json content
   * @param serviceAccountType type of service account can be filePath or json
   *
   * @return {@link Map} properties genereated based on input params
   */
  public static Map<String, String> generateBigQueryAuthProperties(@Nullable String serviceAccount,
                                                                   String serviceAccountType) {
    // mapred.bq prefix is for BigQueryFactory.createBigQueryCredential(), used by the BigQueryOutputFormat
    return generateAuthProperties(serviceAccount, serviceAccountType, "fs.gs", "mapred.bq");
  }

  /**
   * Generate auth related properties to set in the Hadoop configuration.
   *
   * @param serviceAccount file path or Json content
   * @param serviceAccountType type of service account can be filePath or json
   * @param prefixes prefixes for the AccessTokenProvider class
   *
   * @return {@link Map} properties genereated based on input params
   */
  private static Map<String, String> generateAuthProperties(@Nullable String serviceAccount,
                                                            String serviceAccountType, String... prefixes) {
    Map<String, String> properties = new HashMap<>();
    properties.put(SERVICE_ACCOUNT_TYPE, serviceAccountType);
    if (serviceAccount != null) {
      properties.put(SERVICE_ACCOUNT, serviceAccount);
    }

    // use a custom AccessTokenProvider that uses the newer
    // AccessTokenProviderClassFromConfigFactory will by default look for
    //   google.cloud.auth.access.token.provider.impl
    // but can be configured to also look for the conf with other prefixes like
    //   gs.fs.auth.access.token.provider.impl
    //   mapred.bq.auth.access.token.provider.impl
    // for use by GCS and BQ.
    for (String prefix : prefixes) {
      properties.put(prefix + AccessTokenProviderClassFromConfigFactory.ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX,
                     ServiceAccountAccessTokenProvider.class.getName());
    }
    return properties;
  }

  /**
   * Loads credentials based on configuration properties set in the conf. This assumes that the properties from
   * {@link #generateAuthProperties(String, String, String...)} were placed in the configuration.
   */
  public static GoogleCredentials loadCredentialsFromConf(Configuration conf) throws IOException {
    String serviceAccountType = conf.get(GCPUtils.SERVICE_ACCOUNT_TYPE);
    String serviceAccount = conf.get(GCPUtils.SERVICE_ACCOUNT);

    GoogleCredentials credentials;
    if (serviceAccount == null) {
      credentials = GoogleCredentials.getApplicationDefault().createScoped();
    } else {
      boolean isFile = SERVICE_ACCOUNT_TYPE_FILE_PATH.equals(serviceAccountType);
      credentials = loadServiceAccountCredentials(serviceAccount, isFile);
    }

    return credentials.createScoped(CredentialFactory.GCS_SCOPES);
  }

  public static Map<String, String> getFileSystemProperties(GCPConnectorConfig config, String path,
                                                            Map<String, String> properties) {
    return getFileSystemProperties(config.getProject(), config.getServiceAccount(), config.getServiceAccountType(),
                                   path, properties);
  }

  private static Map<String, String> getFileSystemProperties(String project, String serviceAccount,
                                                             String serviceAccountType, String path,
                                                             Map<String, String> properties) {
    try {
      properties.putAll(generateGCSAuthProperties(serviceAccount, serviceAccountType));
    } catch (Exception ignored) {

    }
    properties.put("fs.gs.impl", GoogleHadoopFileSystem.class.getName());
    properties.put("fs.AbstractFileSystem.gs.impl", GoogleHadoopFS.class.getName());
    properties.put(FS_GS_PROJECT_ID, project);
    properties.put("fs.gs.system.bucket", GCSPath.from(path).getBucket());
    properties.put("fs.gs.path.encoding", "uri-path");
    properties.put("fs.gs.working.dir", GCSPath.ROOT_DIR);
    properties.put("fs.gs.impl.disable.cache", "true");
    return properties;
  }

  public static BigQuery getBigQuery(String project, @Nullable Credentials credentials) {
    BigQueryOptions.Builder bigqueryBuilder = BigQueryOptions.newBuilder().setProjectId(project);
    if (credentials != null) {
      Set<String> scopes = new HashSet<>(BIGQUERY_SCOPES);

      if (credentials instanceof ServiceAccountCredentials) {
        scopes.addAll(((ServiceAccountCredentials) credentials).getScopes());
      } else if (credentials instanceof ExternalAccountCredentials) {
        Collection<String> currentScopes = ((ExternalAccountCredentials) credentials).getScopes();
        if (currentScopes != null) {
          scopes.addAll(currentScopes);
        }
      }

      if (credentials instanceof GoogleCredentials) {
        credentials = ((GoogleCredentials) credentials).createScoped(scopes);
      }
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
                                  @Nullable CryptoKeyName cmekKeyName) throws StorageException {
    BucketInfo.Builder builder = BucketInfo.newBuilder(bucket);
    if (location != null) {
      builder.setLocation(location);
    }
    if (cmekKeyName != null) {
      builder.setDefaultKmsKeyName(cmekKeyName.toString());
    }
    storage.create(builder.build());
  }
}
