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
package co.cask.gcp.datastore.source;

import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.gcp.datastore.source.util.DatastoreSourceQueryUtil;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.CONFIG_KIND;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.CONFIG_NAMESPACE;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.CONFIG_NUM_SPLITS;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.CONFIG_PROJECT;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.CONFIG_QUERY;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.CONFIG_SERVICE_ACCOUNT_FILE_PATH;

/**
 * Provides DatastoreInputFormat class name and configuration.
 */
public class DatastoreInputFormatProvider implements InputFormatProvider {

  private final Map<String, String> configMap;

  public DatastoreInputFormatProvider(DatastoreSourceConfig config) {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
      .put(CONFIG_PROJECT, config.getProject())
      .put(CONFIG_NAMESPACE, config.getNamespace())
      .put(CONFIG_KIND, config.getKind())
      .put(CONFIG_QUERY, DatastoreSourceQueryUtil.constructPbQuery(config).toString())
      .put(CONFIG_NUM_SPLITS, String.valueOf(config.getNumSplits()));

    if (Objects.nonNull(config.getServiceAccountFilePath())) {
      builder.put(CONFIG_SERVICE_ACCOUNT_FILE_PATH, config.getServiceAccountFilePath());
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
