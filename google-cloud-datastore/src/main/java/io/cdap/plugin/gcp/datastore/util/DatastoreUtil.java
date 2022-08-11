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
package io.cdap.plugin.gcp.datastore.util;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.common.base.Strings;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreOptions;
import io.cdap.plugin.gcp.datastore.exception.DatastoreInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;

/**
 * Utility class that provides methods to connect to Datastore instance.
 */
public class DatastoreUtil {
  private static final Logger LOG = LoggerFactory.getLogger(DatastoreUtil.class);

  /**
   * Connects to Datastore V1 instance using given credentials in JSON file and project ID.
   *
   * @param serviceAccount path to credentials defined in JSON file
   * @param isServiceAccountFilePath indicator if provided service account if file path or JSON
   * @param projectId Google Cloud project ID
   * @return Datastore service
   */
  public static Datastore getDatastoreV1(@Nullable String serviceAccount,
                                         @Nullable Boolean isServiceAccountFilePath,
                                         String projectId) {
    try {
      final Credential credential = getCredential(serviceAccount, isServiceAccountFilePath);
      DatastoreOptions options =
        new DatastoreOptions.Builder()
          .credential(credential)
          .projectId(projectId)
          .build();

      return DatastoreFactory.get().create(options);
    } catch (IOException e) {
      throw new DatastoreInitializationException("Unable to connect to Datastore V1", e);
    }
  }

  /**
   * Obtains Google Cloud credential from given JSON file.
   * If give path is null or empty, obtains application default credentials.
   *
   * @param serviceAccount path to credentials defined in JSON file
   * @param isServiceAccountFilePath indicator whether service account is file path or JSON
   * @return Google Cloud credential
   * @throws IOException if the credential cannot be created in the current environment
   */
  private static Credential getCredential(@Nullable String serviceAccount,
                                          @Nullable Boolean isServiceAccountFilePath) throws IOException {
    GoogleCredential credential;
    if (!Strings.isNullOrEmpty(serviceAccount)) {
      if (!isServiceAccountFilePath) {
        credential = loadCredentialFromStream(serviceAccount);
      } else {
        credential = loadCredentialFromFile(serviceAccount);
      }
    } else {
      credential = GoogleCredential.getApplicationDefault();
    }
    return credential.createScoped(DatastoreOptions.SCOPES);
  }

  /**
   * Generate credentials from JSON key
   * @param serviceAccountFilePath path to service account file
   * @return Google Cloud credential
   * @throws IOException if the credential cannot be created in the current environment
   */
  private static GoogleCredential loadCredentialFromFile(@Nullable String serviceAccountFilePath) throws IOException {
    try (InputStream inputStream = new FileInputStream(serviceAccountFilePath)) {
      return GoogleCredential.fromStream(inputStream);
    }
  }

  /**
   * Generate credentials from JSON key
   * @param serviceAccount contents of service account JSON
   * @return Google Cloud credential
   * @throws IOException if the credential cannot be created in the current environment
   */
  private static GoogleCredential loadCredentialFromStream(@Nullable String serviceAccount) throws IOException {
    try (InputStream jsonInputStream = new ByteArrayInputStream(serviceAccount.getBytes())) {
      return GoogleCredential.fromStream(jsonInputStream);
    }
  }
}
