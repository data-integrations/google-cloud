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

package io.cdap.plugin.gcp.bigquery.sqlengine.output;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.sink.BigQueryOutputFormatProvider;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * Output Format Provider for BigQuery SQL Engine.
 *
 * If overrides the Output Format class from {@link BigQueryOutputFormatProvider} in order to use
 * {@link BigQuerySQLEngineOutputFormat}.
 */
public class BigQuerySQLEngineOutputFormatProvider extends BigQueryOutputFormatProvider {
  /**
   * @param configuration Hadoop Configuration object, must be set up by using {@link BigQuerySinkUtils#configureOutput}
   * @param tableSchema CDAP Schema for the output table.
   */
  public BigQuerySQLEngineOutputFormatProvider(Configuration configuration,
                                      Schema tableSchema) {
    super(configuration, tableSchema);
  }

  @Override
  public String getOutputFormatClassName() {
    return BigQuerySQLEngineOutputFormat.class.getName();
  }
}
