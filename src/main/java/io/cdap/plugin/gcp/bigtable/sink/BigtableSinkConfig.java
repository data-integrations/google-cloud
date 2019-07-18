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
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.gcp.common.ConfigUtil;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;

import java.io.File;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Holds configuration required for configuring {@link BigtableSink}.
 */
public final class BigtableSinkConfig extends GCPReferenceSourceConfig {
  public static final String TABLE = "table";
  public static final String INSTANCE = "instance";
  public static final String KEY_ALIAS = "keyAlias";
  public static final String COLUMN_MAPPINGS = "columnMappings";

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
  @Description("Mappings from record field to Bigtable column name. Column name format: <family>:<qualifier>.")
  @Macro
  final String columnMappings;

  public BigtableSinkConfig(String table,
                            String instance,
                            String keyAlias,
                            String columnMappings) {
    this.table = table;
    this.instance = instance;
    this.keyAlias = keyAlias;
    this.columnMappings = columnMappings;
  }

  public void validate() {
    super.validate();
    if (!containsMacro(PROJECT) && tryGetProject() == null) {
      throw new InvalidConfigPropertyException("Could not detect Google Cloud project id from the environment. " +
                                                 "Please specify a project id.", PROJECT);
    }
    if (!containsMacro(INSTANCE) && Strings.isNullOrEmpty(instance)) {
      throw new InvalidConfigPropertyException("Instance ID must be specified", INSTANCE);
    }
    String serviceAccountFilePath = getServiceAccountFilePath();
    if (!containsMacro(SERVICE_ACCOUNT_FILE_PATH) && serviceAccountFilePath != null) {
      File serviceAccountFile = new File(serviceAccountFilePath);
      if (!serviceAccountFile.exists()) {
        throw new InvalidConfigPropertyException(String.format("File '%s' does not exist", serviceAccountFilePath),
                                                 SERVICE_ACCOUNT_FILE_PATH);
      }
    }
  }

  @Nullable
  public Map<String, String> getColumnMappings() {
    return columnMappings == null ? null : ConfigUtil.parseKeyValueConfig(columnMappings, ",", "=");
  }

  public boolean connectionParamsConfigured() {
    return !containsMacro(INSTANCE) && Strings.isNullOrEmpty(instance)
      && !containsMacro(PROJECT) && Strings.isNullOrEmpty(project)
      && !containsMacro(TABLE) && Strings.isNullOrEmpty(table)
      && !containsMacro(SERVICE_ACCOUNT_FILE_PATH);
  }
}
