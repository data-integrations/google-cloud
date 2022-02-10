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

import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;

import javax.annotation.Nullable;

/**
 * Interface for datasets used by the BigQuery SQL Engine.
 *
 * These methods are used to ensure that the cleanup removes all GCS object and BigQuery tables.
 */
public interface BigQuerySQLDataset extends SQLDataset {

  /**
   * Return table name where records for this dataset are stored in BigQuery.
   * @return Table names for these records.
   */
  String getBigQueryTableName();

  /**
   * Gets the Job ID for the jobs that are triggered by this Dataset operation.
   * @return Job ID
   */
  @Nullable
  String getJobId();

  /**
   * Gets the GCS path used for temporary storage
   * @return Temporary GCS path, or null if this dataset doesn't need temporary storage.
   */
  @Nullable
  String getGCSPath();
}
