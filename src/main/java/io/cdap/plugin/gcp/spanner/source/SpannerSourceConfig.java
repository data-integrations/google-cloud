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

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.spanner.SpannerConstants;
import io.cdap.plugin.gcp.spanner.common.SpannerUtil;

import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Spanner source config
 */
public class SpannerSourceConfig extends PluginConfig {
  private static final Set<Schema.Type> SUPPORTED_TYPES = ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.STRING,
                                                                          Schema.Type.LONG, Schema.Type.DOUBLE,
                                                                          Schema.Type.BYTES, Schema.Type.ARRAY);

  public static final String NAME_MAX_PARTITIONS = "maxPartitions";
  public static final String NAME_PARTITION_SIZE_MB = "partitionSizeMB";
  public static final String NAME_INSTANCE = "instance";
  public static final String NAME_DATABASE = "database";
  public static final String NAME_TABLE = "table";
  public static final String NAME_IMPORT_QUERY = "importQuery";
  public static final String NAME_SCHEMA = "schema";

  @Name(Constants.Reference.REFERENCE_NAME)
  @Nullable
  @Description("This will be used to uniquely identify this source for lineage, annotating metadata, etc.")
  public String referenceName;

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

  @Description("The SELECT query to use to import data from the specified table.")
  @Macro
  @Nullable
  public String importQuery;

  @Description("Schema of the Spanner table.")
  @Macro
  @Nullable
  public String schema;

  @Name(ConfigUtil.NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  private Boolean useConnection;

  @Name(ConfigUtil.NAME_CONNECTION)
  @Macro
  @Nullable
  @Description("The existing connection to use.")
  private GCPConnectorConfig connection;

  public String getProject() {
    if (connection == null) {
      throw new IllegalArgumentException(
        "Could not get project information, connection should not be null!");
    }
    return connection.getProject();
  }

  @Nullable
  public String tryGetProject() {
    return connection == null ? null : connection.tryGetProject();
  }

  @Nullable
  public String getServiceAccount() {
    return connection == null ? null : connection.getServiceAccount();
  }

  @Nullable
  public Boolean isServiceAccountFilePath() {
    return connection == null ? null : connection.isServiceAccountFilePath();
  }

  @Nullable
  public String getServiceAccountType() {
    return connection == null ? null : connection.getServiceAccountType();
  }

  @Nullable
  public GCPConnectorConfig getConnection() {
    return connection;
  }

  /**
   * Return reference name if provided, otherwise, normalize the FQN and return it as reference name
   * @return referenceName (if provided)/normalized FQN
   */
  public String getReferenceName() {
    return Strings.isNullOrEmpty(referenceName) ? ReferenceNames.normalizeFqn(getFQN()) : referenceName;
  }

  /**
   * Return true if the service account is set to auto-detect but it can't be fetched from the environment.
   * This shouldn't result in a deployment failure, as the credential could be detected at runtime if the pipeline
   * runs on dataproc. This should primarily be used to check whether certain validation logic should be skipped.
   *
   * @return true if the service account is set to auto-detect but it can't be fetched from the environment.
   */
  public boolean autoServiceAccountUnavailable() {
    if (connection == null || connection.getServiceAccountFilePath() == null &&
      connection.isServiceAccountFilePath()) {
      try {
        ServiceAccountCredentials.getApplicationDefault();
      } catch (IOException e) {
        return true;
      }
    }
    return false;
  }

  public void validate(FailureCollector collector) {
    if (!Strings.isNullOrEmpty(referenceName)) {
      IdUtils.validateReferenceName(referenceName, collector);
    }
    ConfigUtil.validateConnection(this, useConnection, connection, collector);
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
  public boolean canConnect() {
    return connection != null && connection.canConnect() && !containsMacro(SpannerSourceConfig.NAME_SCHEMA) &&
      !containsMacro(SpannerSourceConfig.NAME_DATABASE) && !containsMacro(SpannerSourceConfig.NAME_TABLE) &&
      !containsMacro(SpannerSourceConfig.NAME_INSTANCE) && !containsMacro(SpannerSourceConfig.NAME_IMPORT_QUERY);
  }

  /**
   * Get fully-qualified name (FQN) for a Spanner table (FQN format: spanner://{instanceId}.{database}.{table}).
   *
   * @return String fqn
   */
  public String getFQN() {
    String secondFQNPart = String.join(".", instance, database, table);
    return SpannerConstants.SPANNER_FQN_PREFIX + secondFQNPart;
  }
}
