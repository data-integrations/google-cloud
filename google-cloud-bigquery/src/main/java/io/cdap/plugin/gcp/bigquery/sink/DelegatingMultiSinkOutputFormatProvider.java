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
package io.cdap.plugin.gcp.bigquery.sink;

import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * Provides {@link DelegatingMultiSinkOutputFormat} to output values for multiple tables when the table schema
 * is not defined as a pipeline argument.
 */
public class DelegatingMultiSinkOutputFormatProvider implements OutputFormatProvider {

  private final Configuration config;

  public DelegatingMultiSinkOutputFormatProvider(Configuration config,
                                                 String filterField,
                                                 String bucketName,
                                                 String projectName,
                                                 String datasetName) {
    this.config = config;
    DelegatingMultiSinkOutputFormat.configure(config, filterField, bucketName, projectName, datasetName);
  }

  @Override
  public String getOutputFormatClassName() {
    return DelegatingMultiSinkOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    Map<String, String> map = BigQueryUtil.configToMap(config);
    map.put(org.apache.hadoop.mapred.JobContext.OUTPUT_KEY_CLASS, AvroKey.class.getName());
    return map;
  }

}
