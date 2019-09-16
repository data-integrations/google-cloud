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

package io.cdap.plugin.gcp.spanner.sink;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;
import io.cdap.plugin.gcp.spanner.common.SpannerUtil;

import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Spanner sink config
 */
public class SpannerSinkConfig extends GCPReferenceSinkConfig {
  private static final int DEFAULT_SPANNER_WRITE_BATCH_SIZE = 100;
  private static final Set<Schema.Type> SUPPORTED_TYPES =
    ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.STRING, Schema.Type.INT, Schema.Type.LONG,
                    Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.BYTES, Schema.Type.ARRAY);

  public static final String NAME_TABLE = "table";
  public static final String NAME_BATCH_SIZE = "batchSize";
  public static final String NAME_INSTANCE = "instance";
  public static final String NAME_DATABASE = "database";
  public static final String NAME_KEYS = "keys";
  public static final String NAME_SCHEMA = "schema";

  @Name(NAME_TABLE)
  @Description("Cloud Spanner table id. Uniquely identifies your table within the Cloud Spanner database")
  @Macro
  private String table;

  @Name(NAME_BATCH_SIZE)
  @Description("Size of the batched writes to the Spanner table. " +
    "When the number of buffered mutations is greater than this batchSize, " +
    "the mutations are written to Spanner table, Default value is 100")
  @Macro
  @Nullable
  private Integer batchSize;

  @Name(NAME_INSTANCE)
  @Description("Cloud Spanner instance id. " +
    "Uniquely identifies Cloud Spanner instance within your Google Cloud Platform project.")
  @Macro
  private String instance;

  @Name(NAME_DATABASE)
  @Description("Cloud Spanner database id. Uniquely identifies your database within the Cloud Spanner instance.")
  @Macro
  private String database;

  @Name(NAME_KEYS)
  @Nullable
  @Description("Primary keys to be used to create spanner table, if the spanner table does not exist.")
  @Macro
  private String keys;

  @Name(NAME_SCHEMA)
  @Description("Schema of the Spanner table.")
  @Macro
  private String schema;

  public SpannerSinkConfig(String referenceName, String table, @Nullable Integer batchSize, String instance,
                           String database, @Nullable String keys, String schema) {
    this.referenceName = referenceName;
    this.table = table;
    this.batchSize = batchSize;
    this.instance = instance;
    this.database = database;
    this.keys = keys;
    this.schema = schema;
  }

  public String getTable() {
    return table;
  }

  public String getInstance() {
    return instance;
  }

  public String getDatabase() {
    return database;
  }

  @Nullable
  public String getKeys() {
    return keys;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);
    Schema schema = getSchema(collector);
    if (!containsMacro(NAME_SCHEMA) && schema != null) {
      // validate output schema
      SpannerUtil.validateSchema(schema, SUPPORTED_TYPES, collector);
    }
    if (!containsMacro(NAME_BATCH_SIZE) && batchSize != null && batchSize < 1) {
      collector.addFailure("Invalid spanner batch size for writes.", "Ensure the value is a positive number.")
        .withConfigProperty(NAME_BATCH_SIZE);
    }
    if (!containsMacro(NAME_KEYS) && keys != null && !containsMacro(NAME_SCHEMA) && schema != null) {
      String[] splitted = keys.split(",");

      for (String key : splitted) {
        if (schema.getField(key.trim()) == null) {
          collector.addFailure(String.format("Primary key field '%s' does not exist in the schema.", key),
                               "Change the Primary key field to be one of the schema fields.")
            .withConfigElement(NAME_KEYS, key);
        }
      }
    }
  }

  /**
   * Returns schema object.
   */
  @Nullable
  public Schema getSchema(FailureCollector collector) {
    if (Strings.isNullOrEmpty(schema)) {
      return null;
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null).withConfigProperty(NAME_SCHEMA);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  public int getBatchSize() {
    return batchSize == null ? DEFAULT_SPANNER_WRITE_BATCH_SIZE : batchSize;
  }
}
