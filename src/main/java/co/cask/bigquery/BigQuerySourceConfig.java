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

package co.cask.bigquery;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Class description here.
 */
public final class BigQuerySourceConfig extends PluginConfig {
  @Name("dataset")
  @Description("Dataset name")
  @Macro
  public String dataset;

  @Name("table")
  @Description("Table to be read")
  @Macro
  public String table;

  @Name("project")
  @Description("Project ID")
  @Macro
  @Nullable
  public String project;

  @Name("bucket")
  @Description("Temporary Google Cloud Storage bucket name. Files will be automatically cleaned after job completion.")
  @Macro
  public String bucket;

  @Name("serviceFilePath")
  @Description("Service Account File Path")
  @Macro
  @Nullable
  public String serviceAccountFilePath;

  @Name("schema")
  @Description("Schema")
  @Macro
  public String schema;

  public BigQuerySourceConfig(String dataset, String table, String project, String bucket, String schema,
                              String serviceAccountFilePath) {
    this.dataset = dataset;
    this.table = table;
    this.project = project;
    this.bucket = bucket;
    this.schema = schema;
    this.serviceAccountFilePath = serviceAccountFilePath;
  }
}
