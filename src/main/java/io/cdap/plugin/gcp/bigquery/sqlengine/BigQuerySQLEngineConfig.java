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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.gcp.bigquery.common.BigQueryBaseConfig;

import javax.annotation.Nullable;

/**
 * Configuration for SQL Engine.
 */
public class BigQuerySQLEngineConfig extends BigQueryBaseConfig {

  public static final String NAME_LOCATION = "location";
  public static final String NAME_RETAIN_TABLES = "retainTables";
  public static final String NAME_TEMP_TABLE_TTL_HOURS = "tempTableTTLHours";

  @Name(NAME_LOCATION)
  @Macro
  @Nullable
  @Description("The location where the big query dataset will get created. " +
    "This value is ignored if the dataset or temporary bucket already exist.")
  protected String location;

  @Name(NAME_RETAIN_TABLES)
  @Macro
  @Nullable
  @Description("Select this option if the pipeline should retain all temporary BigQuery tables created during the " +
    "execution of the pipeline.")
  protected Boolean retainTables;

  @Name(NAME_TEMP_TABLE_TTL_HOURS)
  @Macro
  @Nullable
  @Description("Set table TTL for temporary BigQuery tables, in number of hours. Tables will be deleted " +
    "automatically on pipeline completion.")
  protected Integer tempTableTTLHours;

  @Nullable
  public String getLocation() {
    return location;
  }

  public Boolean shouldRetainTables() {
    return retainTables != null ? retainTables : false;
  }

  public Integer getTempTableTTLHours() {
    return tempTableTTLHours != null && tempTableTTLHours > 0 ? tempTableTTLHours : 72;
  }
}
