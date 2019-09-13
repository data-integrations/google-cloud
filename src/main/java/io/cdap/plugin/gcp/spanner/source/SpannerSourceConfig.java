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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
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

  public static final String NAME_MAX_PARTITIONS = "maxPartitions";
  public static final String NAME_PARTITION_SIZE_MB = "partitionSizeMB";
  public static final String NAME_INSTANCE = "instance";
  public static final String NAME_DATABASE = "database";
  public static final String NAME_TABLE = "table";
  public static final String NAME_SCHEMA = "schema";

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

  public void validate(FailureCollector collector) {
    super.validate(collector);
    Schema schema = getSchema(collector);
    if (!containsMacro(NAME_SCHEMA) && schema != null) {
      SpannerUtil.validateSchema(schema, SUPPORTED_TYPES, collector);
    }
    if (!containsMacro(NAME_MAX_PARTITIONS) && maxPartitions != null && maxPartitions < 1) {
      collector.addFailure("Invalid max partitions.", "Ensure the value is a positive number.")
        .withConfigProperty(NAME_MAX_PARTITIONS);
    }
    if (!containsMacro(NAME_PARTITION_SIZE_MB) && partitionSizeMB != null && partitionSizeMB < 1) {
      collector.addFailure("Invalid partition size in mega bytes.", "Ensure the value is a positive number.")
        .withConfigProperty(NAME_PARTITION_SIZE_MB);
    }
  }

  @Nullable
  public Schema getSchema(FailureCollector collector) {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null).withConfigProperty(NAME_SCHEMA);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  /**
   * Returns true if spanner table can be connected to or schema is not a macro.
   */
  public boolean shouldConnect() {
    return !containsMacro(SpannerSourceConfig.NAME_SCHEMA) && !containsMacro(SpannerSourceConfig.NAME_DATABASE) &&
      !containsMacro(SpannerSourceConfig.NAME_TABLE) && !containsMacro(SpannerSourceConfig.NAME_INSTANCE) &&
      !containsMacro(SpannerSourceConfig.NAME_SERVICE_ACCOUNT_FILE_PATH) &&
      !containsMacro(SpannerSourceConfig.NAME_PROJECT);
  }
}
