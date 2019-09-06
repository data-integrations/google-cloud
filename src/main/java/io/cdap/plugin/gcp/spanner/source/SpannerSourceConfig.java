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
    if (!containsMacro("schema") && schema != null) {
      SpannerUtil.validateSchema(getSchema(collector), SUPPORTED_TYPES, collector);
    }
    if (!containsMacro("maxPartitions") && maxPartitions != null && maxPartitions < 1) {
      collector.addFailure("Max partitions must be a positive number > 0", null).withConfigProperty("maxPartitions");
    }
    if (!containsMacro("partitionSizeMB") && partitionSizeMB != null && partitionSizeMB < 1) {
      collector.addFailure("Partition size in mega bytes must be a positive number > 0", null)
        .withConfigProperty("partitionSizeMB");
    }
  }

  @Nullable
  public Schema getSchema(FailureCollector collector) {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(),
                           "Provided schema can be parsed correctly.").withConfigProperty("schema");
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }
}
