/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.source;

import com.google.cloud.hadoop.io.bigquery.AvroBigQueryInputFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryFactory;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Override for {@link AvroBigQueryInputFormat} with additional scopes for External tables.
 */
public class AvroBigQueryInputFormatWithScopes extends AvroBigQueryInputFormat {

  /**
   * Override to support additonal scopes, useful when exporting from external tables
   *
   * @param config Hadoop config
   * @return BigQuery Helper instance
   * @throws IOException on IO Error.
   * @throws GeneralSecurityException on security exception.
   */
  @Override
  protected BigQueryHelper getBigQueryHelper(Configuration config) throws GeneralSecurityException, IOException {
    BigQueryFactory factory = new BigQueryFactoryWithScopes(GCPUtils.BIGQUERY_SCOPES);
    return factory.getBigQueryHelper(config);
  }
}
