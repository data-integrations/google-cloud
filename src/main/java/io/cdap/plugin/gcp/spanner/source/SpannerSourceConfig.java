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

package io.cdap.plugin.gcp.spanner.source;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;
import io.cdap.plugin.gcp.spanner.common.SpannerUtil;

import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Spanner source config
 */
public class SpannerSourceConfig extends GCPReferenceSourceConfig {
  private static final Set<Schema.Type> SUPPORTED_TYPES = ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.STRING,
                                                                          Schema.Type.LONG, Schema.Type.DOUBLE,
                                                                          Schema.Type.BYTES, Schema.Type.ARRAY);

  @Description("Maximum number of partitions. This is only a hint. The actual number of partitions may vary")
  @Macro
  @Nullable
  public Long maxPartitions;

  @Description("Partition size in megabytes. This is only a hint. The actual partition size may vary")
  @Macro
  @Nullable
  public Long partitionSizeMB;

  @Description("Cloud Spanner instance id. " +
    "Uniquely identifies Cloud Spanner instance within your Google Cloud Platform project.")
  @Macro
  public String instance;

  @Description("Cloud Spanner database id. Uniquely identifies your database within the Cloud Spanner instance.")
  @Macro
  public String database;

  @Description("Cloud Spanner table id. Uniquely identifies your table within the Cloud Spanner database")
  @Macro
  public String table;

  @Description("Schema of the Spanner table.")
  @Macro
  @Nullable
  public String schema;

  public void validate() {
    super.validate();
    if (!containsMacro("schema") && schema != null) {
      SpannerUtil.validateSchema(getSchema(), SUPPORTED_TYPES);
    }
    if (!containsMacro("maxPartitions") && maxPartitions != null && maxPartitions < 1) {
      throw new InvalidConfigPropertyException("Max partitions should be positive", "maxPartitions");
    }
    if (!containsMacro("partitionSizeMB") && partitionSizeMB != null && partitionSizeMB < 1) {
      throw new InvalidConfigPropertyException("Partition size in mega bytes should be positive", "partitionSizeMB");
    }
  }

  @Nullable
  public Schema getSchema() {
    try {
      return schema == null ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema: " + e.getMessage(), e);
    }
  }
}
