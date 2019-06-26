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

package io.cdap.plugin.gcp.bigtable.source;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.gcp.common.ErrorHandling;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Holds configuration required for configuring {@link BigtableSource}.
 */
public final class BigtableSourceConfig extends GCPReferenceSourceConfig {
  public static final String TABLE = "table";
  public static final String INSTANCE = "instance";
  public static final String KEY_ALIAS = "keyAlias";
  public static final String SCAN_ROW_START = "scanRowStart";
  public static final String SCAN_ROW_STOP = "scanRowStop";
  public static final String SCAN_TIME_RANGE_START = "scanTimeRangeStart";
  public static final String SCAN_TIME_RANGE_STOP = "scanTimeRangeStop";
  public static final String SCHEMA = "schema";
  public static final String ON_ERROR = "on-error";

  private static final Set<Schema.Type> SUPPORTED_FIELD_TYPES = ImmutableSet.of(
    Schema.Type.BOOLEAN,
    Schema.Type.INT,
    Schema.Type.LONG,
    Schema.Type.FLOAT,
    Schema.Type.DOUBLE,
    Schema.Type.BYTES,
    Schema.Type.STRING
  );

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
  @Nullable
  final String keyAlias;

  @Name(SCAN_ROW_START)
  @Description("Scan start row.")
  @Macro
  @Nullable
  final String scanRowStart;

  @Name(SCAN_ROW_STOP)
  @Description("Scan stop row.")
  @Macro
  @Nullable
  final String scanRowStop;

  @Name(SCAN_TIME_RANGE_START)
  @Description("Starting timestamp used to filter columns with a specific range of versions. Inclusive.")
  @Macro
  @Nullable
  final Long scanTimeRangeStart;

  @Name(SCAN_TIME_RANGE_STOP)
  @Description("Ending timestamp used to filter columns with a specific range of versions. Exclusive.")
  @Macro
  @Nullable
  final Long scanTimeRangeStop;

  @Name(ON_ERROR)
  @Description("How to handle error in record processing. " +
    "Error will be thrown if failed to parse value according to provided schema.")
  @Macro
  final String onError;

  @Name(SCHEMA)
  @Macro
  @Description("The schema of the table to read.")
  final String schema;

  public BigtableSourceConfig(String table, String instance, @Nullable String project,
                              @Nullable String serviceFilePath, @Nullable String keyAlias,
                              @Nullable String scanRowStart, @Nullable String scanRowStop,
                              @Nullable Long scanTimeRangeStart, @Nullable Long scanTimeRangeStop,
                              String onError, String schema) {
    this.table = table;
    this.instance = instance;
    this.project = project;
    this.serviceFilePath = serviceFilePath;
    this.keyAlias = keyAlias;
    this.scanRowStart = scanRowStart;
    this.scanRowStop = scanRowStop;
    this.scanTimeRangeStart = scanTimeRangeStart;
    this.scanTimeRangeStop = scanTimeRangeStop;
    this.onError = onError;
    this.schema = schema;
  }

  public void validate() {
    super.validate();
    if (!containsMacro(PROJECT) && null == tryGetProject()) {
      throw new InvalidConfigPropertyException("Could not detect Google Cloud project id from the environment. " +
                                                 "Please specify a project id.", PROJECT);
    }
    if (!containsMacro(INSTANCE) && Strings.isNullOrEmpty(instance)) {
      throw new InvalidConfigPropertyException("Instance ID must be specified", INSTANCE);
    }
    String serviceAccountFilePath = getServiceAccountFilePath();
    if (!containsMacro(SERVICE_ACCOUNT_FILE_PATH) && null != serviceAccountFilePath) {
      File serviceAccountFile = new File(serviceAccountFilePath);
      if (!serviceAccountFile.exists()) {
        throw new InvalidConfigPropertyException(
          String.format("Service account file '%s' does not exist", serviceAccountFilePath),
          SERVICE_ACCOUNT_FILE_PATH
        );
      }
    }
    if (!containsMacro(ON_ERROR) && null != onError) {
      if (Strings.isNullOrEmpty(onError)) {
        throw new InvalidConfigPropertyException("Error handling must be specified", ON_ERROR);
      }
      if (null == ErrorHandling.fromDisplayName(onError)) {
        throw new InvalidConfigPropertyException("Invalid record error handling strategy name", ON_ERROR);
      }
    }
    if (!containsMacro(SCHEMA)) {
      Schema parsedSchema = getSchema();
      if (null == parsedSchema) {
        throw new InvalidConfigPropertyException("Schema must be specified", SCHEMA);
      }
      if (Schema.Type.RECORD != parsedSchema.getType()) {
        String message = String.format("Schema is of invalid type '%s'. The schema must be a record.",
                                       parsedSchema.getType());
        throw new InvalidConfigPropertyException(message, SCHEMA);
      }
      List<Schema.Field> fields = parsedSchema.getFields();
      if (null == fields || fields.isEmpty()) {
        throw new InvalidConfigPropertyException("Schema should contain fields to map", SCHEMA);
      }
      for (Schema.Field field : fields) {
        Schema.Type fieldType = field.getSchema().isNullable() ?
          field.getSchema().getNonNullable().getType() : field.getSchema().getType();
        if (!SUPPORTED_FIELD_TYPES.contains(fieldType)) {
          String supportedTypes = SUPPORTED_FIELD_TYPES.stream()
            .map(Enum::name)
            .map(String::toLowerCase)
            .collect(Collectors.joining(", "));
          String errorMessage = String.format("Field '%s' is of unsupported type '%s'. Supported types are: %s. ",
                                              field.getName(), fieldType, supportedTypes);
          throw new InvalidConfigPropertyException(errorMessage, SCHEMA);
        }
        if (field.getName().equals(keyAlias)) {
          // key should be defined in schema but will not be retrieved from column families.
          continue;
        }
        try {
          HBaseColumn.fromFullName(field.getName());
        } catch (IllegalArgumentException e) {
          String errorMessage = String.format("Field '%s' has invalid name. Reason: %s",
                                              field.getName(), e.getMessage());
          throw new InvalidConfigPropertyException(errorMessage, SCHEMA);
        }
      }
    }
  }

  /**
   * @return the schema of the dataset
   */
  @Nullable
  public Schema getSchema() {
    try {
      return null == schema ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new InvalidConfigPropertyException("Invalid schema: " + e.getMessage(), SCHEMA);
    }
  }

  public ErrorHandling getErrorHandling() {
    return Objects.requireNonNull(ErrorHandling.fromDisplayName(onError));
  }

  public boolean connectionParamsConfigured() {
    return !containsMacro(INSTANCE) && Strings.isNullOrEmpty(instance)
      && !containsMacro(PROJECT) && Strings.isNullOrEmpty(project)
      && !containsMacro(TABLE) && Strings.isNullOrEmpty(table)
      && !containsMacro(SERVICE_ACCOUNT_FILE_PATH);
  }

  public List<HBaseColumn> getRequestedColumns() {
    List<HBaseColumn> families = new ArrayList<>();
    Schema parsedSchema = getSchema();
    if (null != parsedSchema) {
      for (Schema.Field field : parsedSchema.getFields()) {
        if (field.getName().equals(keyAlias)) {
          // key should be defined in schema but will not be retrieved from column families.
          continue;
        }
        HBaseColumn hBaseColumn = HBaseColumn.fromFullName(field.getName());
        families.add(hBaseColumn);
      }
    }
    return families;
  }
}
