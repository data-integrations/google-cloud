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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.common.BigQueryBaseConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

/**
 * Output Format provider for BigQuery
 */
public class BigQueryOutputFormatProvider implements OutputFormatProvider {

  protected Configuration configuration;
  protected Schema tableSchema;

  /**
   * @param configuration Hadoop Configuration object, must be set up by using
   *                      {@link BigQuerySinkUtils#getOutputConfiguration}
   * @param tableSchema CDAP Schema for the output table.
   */
  public BigQueryOutputFormatProvider(Configuration configuration,
                                      Schema tableSchema) {
    this.configuration = configuration;
    this.tableSchema = tableSchema;
  }

  @Override
  public String getOutputFormatClassName() {
    return BigQueryOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    Map<String, String> configToMap = BigQueryUtil.configToMap(configuration);
    if (tableSchema != null) {
      configToMap
        .put(BigQueryConstants.CDAP_BQ_SINK_OUTPUT_SCHEMA, tableSchema.toString());
    }
    return configToMap;
  }
}
