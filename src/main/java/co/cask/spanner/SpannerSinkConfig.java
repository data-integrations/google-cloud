/*
 * Copyright © 2018 Google Inc.
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

package co.cask.spanner;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.ReferencePluginConfig;

import java.io.IOException;

/**
 * This class <code>Spanner</code> provides all the configuration required for
 * configuring the <code>BigQuerySink</code> plugin.
 */
public final class SpannerSinkConfig extends ReferencePluginConfig {
  @Name("project")
  @Description("Project ID")
  @Macro
  public String project;

  @Name("instanceid")
  @Description("Spanner Instance Id")
  @Macro
  public String instanceid;

  @Name("database")
  @Description("Database to be used")
  @Macro
  public String database;

  @Name("table")
  @Description("Table to write to")
  @Macro
  public String table;

  @Name("serviceFilePath")
  @Description("Service Account File Path")
  @Macro
  public String serviceAccountFilePath;

  @Name("schema")
  @Description("Schema of the BigQuery table")
  public String schema;

  public SpannerSinkConfig(String referenceName) {
    super(referenceName);
  }

  public Schema getSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema.");
    }
  }
}
