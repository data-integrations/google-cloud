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

package io.cdap.plugin.gcp.firestore.util;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.common.base.Strings;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.firestore.exception.FirestoreInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Utility class that provides methods to connect to Firestore instance.
 */
public class FirestoreUtil {
  private static final Logger LOG = LoggerFactory.getLogger(FirestoreUtil.class);

  /**
   * Connects to Firestore instance using given credentials in JSON file, project ID and optional database Id.
   *
   * @param serviceAccountFilePath path to credentials defined in JSON file
   * @param projectId Google Cloud project ID
   * @param databaseId Firestore Database ID
   * @return Firestore service
   */
  public static Firestore getFirestore(@Nullable String serviceAccountFilePath, String projectId,
                                       @Nullable String databaseId) {
    try {
      FirestoreOptions.Builder optionsBuilder = FirestoreOptions.newBuilder()
        .setProjectId(projectId);

      LOG.debug("serviceAccount={}, project={}, db={}...", serviceAccountFilePath, projectId, databaseId);

      if (!Strings.isNullOrEmpty(serviceAccountFilePath)) {
        optionsBuilder.setCredentials(GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath));
      }

      if (!Strings.isNullOrEmpty(databaseId)) {
        optionsBuilder.setDatabaseId(databaseId);
      }

      return optionsBuilder.build().getService();
    } catch (IOException e) {
      throw new FirestoreInitializationException("Unable to connect to Firestore", e);
    }
  }
}
