/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.dataplex.source;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.plugin.common.batch.ConfigurationUtils;
import io.cdap.plugin.gcp.bigquery.source.PartitionedBigQueryInputFormat;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * InputFormatProvider for DataplexSource
 */
public class DataplexInputFormatProvider implements InputFormatProvider {

  public static final String DATAPLEX_ENTITY_TYPE = "dataplex.source.entity.type";
  private final String inputFormatClassName;
  private final Configuration configuration;
  protected Map<String, String> inputFormatConfiguration;

  /**
   * @param inputFormatClassName it will be null for Asset type : BQ asset type
   */
  public DataplexInputFormatProvider(Configuration conf, String inputFormatClassName) {
    this.configuration = conf;
    this.inputFormatClassName = inputFormatClassName;

    if (configuration != null) {
      String assetType = configuration.get(DATAPLEX_ENTITY_TYPE);
      if (assetType.equalsIgnoreCase(DataplexBatchSource.BIGQUERY_DATASET_ENTITY_TYPE)) {
        this.inputFormatConfiguration = StreamSupport
          .stream(configuration.spliterator(), false)
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      } else if (assetType.equalsIgnoreCase(DataplexBatchSource.STORAGE_BUCKET_ENTITY_TYPE)) {
        this.inputFormatConfiguration = ConfigurationUtils.getNonDefaultConfigurations(configuration);
      }
    }

  }


  @Override
  public String getInputFormatClassName() {
    if (configuration != null) {
      String assetType = configuration.get(DATAPLEX_ENTITY_TYPE);
      if (assetType.equalsIgnoreCase(DataplexBatchSource.BIGQUERY_DATASET_ENTITY_TYPE)) {
        return PartitionedBigQueryInputFormat.class.getName();
      } else if (assetType.equalsIgnoreCase(DataplexBatchSource.STORAGE_BUCKET_ENTITY_TYPE)) {
        return this.inputFormatClassName;
      }
    }
    return this.inputFormatClassName;
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return this.inputFormatConfiguration;
  }

}
