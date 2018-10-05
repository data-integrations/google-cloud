/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.gcp.spanner.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.common.GCPReferenceSinkConfig;
import co.cask.gcp.spanner.common.SpannerUtil;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Spanner sink config
 */
public class SpannerSinkConfig extends GCPReferenceSinkConfig {
  private static final int DEFAULT_SPANNER_WRITE_BATCH_SIZE = 100;

  @Name("table")
  @Description("Cloud Spanner table id. Uniquely identifies your table within the Cloud Spanner database")
  @Macro
  public String table;

  @Name("batchSize")
  @Description("Size of the batched writes to the Spanner table. " +
    "When the number of buffered mutations is greater than this batchSize, " +
    "the mutations are written to Spanner table, Default value is 100")
  @Macro
  @Nullable
  public Integer batchSize;

  @Description("Cloud Spanner instance id. " +
    "Uniquely identifies Cloud Spanner instance within your Google Cloud Platform project.")
  @Macro
  public String instance;

  @Description("Cloud Spanner database id. Uniquely identifies your database within the Cloud Spanner instance.")
  @Macro
  public String database;

  @Description("Schema of the Spanner table.")
  @Macro
  public String schema;

  public void validate() {
    super.validate();
    if (!containsMacro("schema")) {
      SpannerUtil.validateSchema(getSchema());
    }
    if (!containsMacro("batchSize") && batchSize != null && batchSize < 1) {
      throw new IllegalArgumentException("Spanner batch size for writes should be positive");
    }
  }

  public Schema getSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema: " + e.getMessage(), e);
    }
  }

  public int getBatchSize() {
    return batchSize == null ? DEFAULT_SPANNER_WRITE_BATCH_SIZE : batchSize;
  }
}
