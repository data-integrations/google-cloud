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
package io.cdap.plugin.gcp.datastore.source;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.plugin.gcp.datastore.source.util.DatastoreSourceConstants;

import java.util.Map;
import java.util.Objects;

/**
 * Provides DatastoreInputFormat class name and configuration.
 */
public class DatastoreInputFormatProvider implements InputFormatProvider {

  private final Map<String, String> configMap;

  public DatastoreInputFormatProvider(String project, String serviceAccountPath, String namespace, String kind,
                                      String query, String spits) {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
      .put(DatastoreSourceConstants.CONFIG_PROJECT, project)
      .put(DatastoreSourceConstants.CONFIG_NAMESPACE, namespace)
      .put(DatastoreSourceConstants.CONFIG_KIND, kind)
      .put(DatastoreSourceConstants.CONFIG_QUERY, query)
      .put(DatastoreSourceConstants.CONFIG_NUM_SPLITS, spits);
    if (Objects.nonNull(serviceAccountPath)) {
      builder.put(DatastoreSourceConstants.CONFIG_SERVICE_ACCOUNT_FILE_PATH, serviceAccountPath);
    }
    this.configMap = builder.build();
  }

  @Override
  public String getInputFormatClassName() {
    return DatastoreInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return configMap;
  }
}
