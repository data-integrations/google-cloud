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

package io.cdap.plugin.gcp.bigquery.sqlengine;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;

import java.util.UUID;

/**
 * Utility Class for the BigQuery SQL Engine implementation.
 */
public class BigQuerySQLEngineUtils {

  public static final String GCS_PATH_FORMAT = BigQuerySinkUtils.GS_PATH_FORMAT + "/%s";
  public static final String BQ_TABLE_NAME_FORMAT = "%s_%s";

  public static String getGCSPath(String bucket, String runId, String tableId) {
    return String.format(GCS_PATH_FORMAT, bucket, runId, tableId);
  }

  public static String getIdentifier() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public static String getNewTableName(String runId) {
    return String.format(BQ_TABLE_NAME_FORMAT, runId, getIdentifier());
  }

  public static Long getNumRows(BigQuery bigQuery, String project, String dataset, String table) {
    TableId tableId = TableId.of(project, dataset, table);
    Table bgTable = bigQuery.getTable(tableId);

    if (bgTable == null) {
      throw new SQLEngineException(String.format("Table '%s' could not be found on dataset '%s' and project `%s`",
                                                 table, dataset, project));
    }

    return bgTable.getNumRows().longValue();
  }
}
