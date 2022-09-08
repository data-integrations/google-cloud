/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.gcp.dataplex.common.util;

/**
 * Dataplex Constants
 */
public final class DataplexConstants {
  public static final String DATAPLEX_TASK_ID = "dataplex.task.id";
  public static final String DATAPLEX_PROJECT_ID = "dataplex.gcp.project.id";
  public static final String DATAPLEX_LOCATION = "dataplex.location.id";
  public static final String DATAPLEX_LAKE = "dataplex.lake.id";
  public static final String DATAPLEX_ENTITY_TYPE = "dataplex.source.entity.type";
  public static final String SERVICE_ACCOUNT_TYPE = "cdap.gcs.auth.service.account.type.flag";
  public static final String SERVICE_ACCOUNT_FILEPATH = "cdap.gcs.auth.service.account.type.filepath";
  public static final String NONE = "none";
  public static final String BIGQUERY_DATASET_ASSET_TYPE = "BIGQUERY_DATASET";
  public static final String STORAGE_BUCKET_ASSET_TYPE = "STORAGE_BUCKET";
  public static final String STORAGE_BUCKET_PARTITION_KEY = "ts";
  public static final String STORAGE_BUCKET_PATH_PREFIX = "gs://";
}
