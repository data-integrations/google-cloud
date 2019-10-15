/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigtable.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.bigtable.common.HBaseColumn;
import io.cdap.plugin.gcp.common.ConfigUtil;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Holds configuration required for configuring {@link BigtableSink}.
 */
public final class BigtableSinkConfig extends GCPReferenceSinkConfig {
  public static final String TABLE = "table";
  public static final String INSTANCE = "instance";
  public static final String KEY_ALIAS = "keyAlias";
  public static final String COLUMN_MAPPINGS = "columnMappings";
  public static final String BIGTABLE_OPTIONS = "bigtableOptions";

  @Name(TABLE)
  @Macro
  @Description("The table to read from. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  final String table;

  @Name(INSTANCE)
  @Macro
  @Description("BigTable instance id. " +
    "Uniquely identifies BigTable instance within your Google Cloud Platform project.")
  final String instance;

  @Name(KEY_ALIAS)
  @Description("Name of the field for row key.")
  @Macro
  final String keyAlias;

  @Name(COLUMN_MAPPINGS)
  @Description("Mappings from record field to Bigtable column name. " +
    "Column names must be formatted as <family>:<qualifier>.")
  @Macro
  private final String columnMappings;

  @Name(BIGTABLE_OPTIONS)
  @Description("Additional connection properties for Bigtable")
  @Macro
  @Nullable
  private final String bigtableOptions;

  public BigtableSinkConfig(String referenceName, String table, String instance, @Nullable String project,
                            @Nullable String serviceFilePath, String keyAlias, String columnMappings,
                            @Nullable String bigtableOptions) {
    this.referenceName = referenceName;
    this.table = table;
    this.instance = instance;
    this.project = project;
    this.serviceFilePath = serviceFilePath;
    this.keyAlias = keyAlias;
    this.columnMappings = columnMappings;
    this.bigtableOptions = bigtableOptions;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (!containsMacro(TABLE) && Strings.isNullOrEmpty(table)) {
      collector.addFailure("Table name must be specified.", null).withConfigProperty(TABLE);
    }
    if (!containsMacro(INSTANCE) && Strings.isNullOrEmpty(instance)) {
      collector.addFailure("Instance ID must be specified.", null).withConfigProperty(INSTANCE);
    }
    String serviceAccountFilePath = getServiceAccountFilePath();
    if (!containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) && serviceAccountFilePath != null) {
      File serviceAccountFile = new File(serviceAccountFilePath);
      if (!serviceAccountFile.exists()) {
        collector.addFailure(String.format("Service account file '%s' does not exist.", serviceAccountFilePath),
                             "Ensure the service account file is available on the local filesystem.")
          .withConfigProperty(NAME_SERVICE_ACCOUNT_FILE_PATH);
      }
    }
  }

  public Map<String, HBaseColumn> getColumnMappings(FailureCollector collector) {
    Map<String, String> specifiedMappings = Strings.isNullOrEmpty(columnMappings) ?
      Collections.emptyMap() : ConfigUtil.parseKeyValueConfig(columnMappings, ",", "=");
    Map<String, HBaseColumn> mappings = new HashMap<>(specifiedMappings.size());

    for (Map.Entry<String, String> entry : specifiedMappings.entrySet()) {
      try {
        String field = entry.getKey();
        HBaseColumn column = HBaseColumn.fromFullName(entry.getValue());
        mappings.put(field, column);
      } catch (IllegalArgumentException e) {
        String errorMessage = String.format("Invalid column in mapping '%s'. Reason: %s",
                                            entry.getKey(), e.getMessage());
        collector.addFailure(errorMessage, "Specify valid column mappings.")
          .withConfigElement(COLUMN_MAPPINGS, ConfigUtil.getKVPair(entry.getKey(), entry.getValue(), "="));
      }
    }
    return mappings;
  }

  public Map<String, String> getBigtableOptions() {
    return Strings.isNullOrEmpty(bigtableOptions) ? Collections.emptyMap() :
      ConfigUtil.parseKeyValueConfig(bigtableOptions, ",", "=");
  }

  public boolean connectionParamsConfigured() {
    return !containsMacro(INSTANCE) && Strings.isNullOrEmpty(instance)
      && !containsMacro(NAME_PROJECT) && Strings.isNullOrEmpty(project)
      && !containsMacro(TABLE) && Strings.isNullOrEmpty(table)
      && !containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH)
      && tryGetProject() != null
      && !autoServiceAccountUnavailable();
  }
}
