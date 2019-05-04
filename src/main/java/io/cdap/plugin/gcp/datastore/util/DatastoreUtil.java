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
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.base.Strings;
import com.google.datastore.v1.client.DatastoreFactory;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.datastore.exception.DatastoreInitializationException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;

/**
 * Utility class that provides methods to connect to Datastore instance.
 */
public class DatastoreUtil {

  /**
   * Connects to Datastore instance using given credentials in JSON file and project ID.
   *
   * @param serviceAccountFilePath path to credentials defined in JSON file
   * @param projectId Google Cloud project ID
   * @return Datastore service
   */
  public static Datastore getDatastore(@Nullable String serviceAccountFilePath, String projectId) {
    try {
      DatastoreOptions.Builder optionsBuilder = DatastoreOptions.newBuilder()
        .setProjectId(projectId);

      if (!Strings.isNullOrEmpty(serviceAccountFilePath)) {
        optionsBuilder.setCredentials(GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath));
      }

      return optionsBuilder.build().getService();
    } catch (IOException e) {
      throw new DatastoreInitializationException("Unable to connect to Datastore", e);
    }
  }

  /**
   * Connects to Datastore V1 instance using given credentials in JSON file and project ID.
   *
   * @param serviceAccountFilePath path to credentials defined in JSON file
   * @param projectId Google Cloud project ID
   * @return Datastore service
   * @throws DatastoreInitializationException when unable to connect to Datastore
   */
  public static com.google.datastore.v1.client.Datastore getDatastoreV1(@Nullable String serviceAccountFilePath,
                                                                        String projectId) {
    try {
      com.google.datastore.v1.client.DatastoreOptions options =
        new com.google.datastore.v1.client.DatastoreOptions.Builder()
          .credential(getCredential(serviceAccountFilePath))
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
   * @param serviceAccountFilePath path to credentials defined in JSON file
   * @return Google Cloud credential
   * @throws IOException if the credential cannot be created in the current environment
   */
  private static Credential getCredential(@Nullable String serviceAccountFilePath) throws IOException {
    GoogleCredential credential;
    if (!Strings.isNullOrEmpty(serviceAccountFilePath)) {
      try (InputStream inputStream = new FileInputStream(serviceAccountFilePath)) {
        credential = GoogleCredential.fromStream(inputStream);
      }
    } else {
      credential = GoogleCredential.getApplicationDefault();
    }
    return credential.createScoped(com.google.datastore.v1.client.DatastoreOptions.SCOPES);
  }
}
