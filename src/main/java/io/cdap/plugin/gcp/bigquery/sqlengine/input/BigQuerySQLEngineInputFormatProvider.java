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

package io.cdap.plugin.gcp.bigquery.sqlengine.input;

import io.cdap.plugin.gcp.bigquery.source.BigQueryInputFormatProvider;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * Input format provider for BigQuery SQL Engine Pull operations.
 */
public class BigQuerySQLEngineInputFormatProvider extends BigQueryInputFormatProvider {

  /**
   * This constructor is only used when Spark serializes this class.
   */
  protected BigQuerySQLEngineInputFormatProvider() {
    // no-op
  }

  public BigQuerySQLEngineInputFormatProvider(Configuration configuration) {
    super(configuration);
  }

  @Override
  public String getInputFormatClassName() {
    return BigQuerySQLEngineInputFormat.class.getName();
  }
}
