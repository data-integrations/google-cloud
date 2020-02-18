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

package io.cdap.plugin.gcp.firestore.source;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.plugin.gcp.firestore.util.Util;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.cdap.plugin.gcp.common.GCPConfig.NAME_PROJECT;
import static io.cdap.plugin.gcp.common.GCPConfig.NAME_SERVICE_ACCOUNT_FILE_PATH;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_CUSTOM_QUERY;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_PULL_DOCUMENTS;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_QUERY_MODE;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_SCHEMA;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_SKIP_DOCUMENTS;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_COLLECTION;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_DATABASE_ID;

/**
 * Provides FirestoreInputFormat class name and configuration.
 */
public class FirestoreInputFormatProvider implements InputFormatProvider {

  private final Map<String, String> configMap;

  public FirestoreInputFormatProvider(String project, String serviceAccountPath, String databaseId, String collection,
                                      String mode, String pullDocuments, String skipDocuments, String filters,
                                      List<String> fields) {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
      .put(NAME_PROJECT, project)
      .put(PROPERTY_DATABASE_ID, Util.isNullOrEmpty(databaseId) ? "" : databaseId)
      .put(PROPERTY_COLLECTION, Util.isNullOrEmpty(collection) ? "" : collection)
      .put(PROPERTY_QUERY_MODE, mode)
      .put(PROPERTY_PULL_DOCUMENTS, Util.isNullOrEmpty(pullDocuments) ? "" : pullDocuments)
      .put(PROPERTY_SKIP_DOCUMENTS, Util.isNullOrEmpty(skipDocuments) ? "" : skipDocuments)
      .put(PROPERTY_CUSTOM_QUERY, Util.isNullOrEmpty(filters) ? "" : filters)
      .put(PROPERTY_SCHEMA, Joiner.on(",").join(fields));
    if (Objects.nonNull(serviceAccountPath)) {
      builder.put(NAME_SERVICE_ACCOUNT_FILE_PATH, serviceAccountPath);
    }
    this.configMap = builder.build();
  }

  @Override
  public String getInputFormatClassName() {
    return FirestoreInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return configMap;
  }
}
