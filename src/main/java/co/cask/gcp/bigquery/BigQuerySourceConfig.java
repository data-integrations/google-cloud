/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.gcp.bigquery;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.gcp.common.GCPReferenceSourceConfig;

import javax.annotation.Nullable;

/**
 * Class description here.
 */
public final class BigQuerySourceConfig extends GCPReferenceSourceConfig {
  @Name("dataset")
  @Description("The dataset the table belongs to. A dataset is contained within a specific project. "
    + "Datasets are top-level containers that are used to organize and control access to tables and views.")
  @Macro
  public String dataset;

  @Name("table")
  @Description("The table to read from. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  @Macro
  public String table;

  @Nullable
  @Name("bucket")
  @Description("The Google Cloud Storage bucket to store temporary data in. "
    + "It will be automatically created if it does not exist, but will not be automatically deleted. "
    + "Temporary data will be deleted after it has been read. " +
    "If it is not provided, a unique bucket will be created and then deleted after the run finishes.")
  @Macro
  public String bucket;

  @Name("schema")
  @Description("The schema of the table to read.")
  @Macro
  public String schema;
}
