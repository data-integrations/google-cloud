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
import com.google.common.collect.Sets;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.bigtable.common.HBaseColumn;
import io.cdap.plugin.gcp.common.ConfigUtil;
import io.cdap.plugin.gcp.common.ErrorHandling;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  public static final String COLUMN_MAPPINGS = "columnMappings";
  public static final String SCAN_ROW_START = "scanRowStart";
  public static final String SCAN_ROW_STOP = "scanRowStop";
  public static final String SCAN_TIME_RANGE_START = "scanTimeRangeStart";
  public static final String SCAN_TIME_RANGE_STOP = "scanTimeRangeStop";
  public static final String BIGTABLE_OPTIONS = "bigtableOptions";
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

  @Name(COLUMN_MAPPINGS)
  @Description("Mappings from Bigtable column name to record field. " +
    "Column names must be formatted as <family>:<qualifier>.")
  @Macro
  @Nullable
  final String columnMappings;

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

  @Name(BIGTABLE_OPTIONS)
  @Description("Additional connection properties for Bigtable")
  @Macro
  @Nullable
  private final String bigtableOptions;

  @Name(ON_ERROR)
  @Description("How to handle error in record processing. " +
    "Error will be thrown if failed to parse value according to provided schema.")
  @Macro
  final String onError;

  @Name(SCHEMA)
  @Macro
  @Description("The schema of the table to read.")
  final String schema;

  public BigtableSourceConfig(String referenceName, String table, String instance, @Nullable String project,
                              @Nullable String serviceFilePath,
                              @Nullable String keyAlias, @Nullable String columnMappings,
                              @Nullable String scanRowStart, @Nullable String scanRowStop,
                              @Nullable Long scanTimeRangeStart, @Nullable Long scanTimeRangeStop,
                              @Nullable String bigtableOptions, String onError, String schema) {
    this.referenceName = referenceName;
    this.table = table;
    this.instance = instance;
    this.columnMappings = columnMappings;
    this.bigtableOptions = bigtableOptions;
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
    if (!containsMacro(ON_ERROR)) {
      if (Strings.isNullOrEmpty(onError)) {
        collector.addFailure("Error handling must be specified.", null).withConfigProperty(ON_ERROR);
      }
      if (!Strings.isNullOrEmpty(onError) && ErrorHandling.fromDisplayName(onError) == null) {
        collector.addFailure(String.format("Invalid record error handling strategy name '%s'.", onError),
                             String.format("Supported error handling strategies are: %s.",
                                           ErrorHandling.getSupportedErrorHandling())).withConfigProperty(ON_ERROR);
      }
    }
    Map<String, String> columnMappings = getColumnMappings();
    if (!containsMacro(COLUMN_MAPPINGS)) {
      if (columnMappings.isEmpty()) {
        collector.addFailure("Column mappings are missing.", "Specify column mappings.")
          .withConfigProperty(COLUMN_MAPPINGS);
      }
      columnMappings.forEach((columnName, fieldName) -> {
        try {
          HBaseColumn.fromFullName(columnName);
        } catch (IllegalArgumentException e) {
          String errorMessage = String.format("Invalid column in mapping '%s'. Reason: %s", columnName, e.getMessage());
          collector.addFailure(errorMessage, "Specify valid column mappings.")
            .withConfigElement(COLUMN_MAPPINGS, ConfigUtil.getKVPair(columnName, columnMappings.get(columnName), "="));
        }
      });
    }
    if (!containsMacro(SCHEMA)) {
      Schema parsedSchema = getSchema(collector);
      if (parsedSchema == null) {
        collector.addFailure("Output schema must be specified.", null).withConfigProperty(SCHEMA);
        throw collector.getOrThrowException();
      }
      if (Schema.Type.RECORD != parsedSchema.getType()) {
        String message = String.format("Schema is of invalid type '%s'.", parsedSchema.getType());
        collector.addFailure(message, "The schema must be a record.").withConfigProperty(SCHEMA);
        throw collector.getOrThrowException();
      }
      List<Schema.Field> fields = parsedSchema.getFields();
      if (null == fields || fields.isEmpty()) {
        collector.addFailure("Schema must contain fields.", null).withConfigProperty(SCHEMA);
        throw collector.getOrThrowException();
      } else {
        if (!columnMappings.isEmpty()) {
          Set<String> mappedFieldNames = Sets.newHashSet(columnMappings.values());
          Set<String> schemaFieldNames = fields.stream()
            .map(Schema.Field::getName)
            .filter(name -> !name.equals(keyAlias))
            .collect(Collectors.toSet());
          Sets.SetView<String> nonMappedColumns = Sets.difference(schemaFieldNames, mappedFieldNames);
          for (String nonMappedColumn : nonMappedColumns) {
            collector.addFailure(
              String.format("Field '%s' does not have corresponding column mapping.", nonMappedColumn),
              String.format("Add column mapping for field '%s'.", nonMappedColumn))
              .withOutputSchemaField(nonMappedColumn);
          }
        }

        for (Schema.Field field : fields) {
          Schema nonNullableSchema = field.getSchema().isNullable() ?
            field.getSchema().getNonNullable() : field.getSchema();

          if (!SUPPORTED_FIELD_TYPES.contains(nonNullableSchema.getType()) ||
            nonNullableSchema.getLogicalType() != null) {
            String supportedTypes = SUPPORTED_FIELD_TYPES.stream()
              .map(Enum::name)
              .map(String::toLowerCase)
              .collect(Collectors.joining(", "));
            String errorMessage = String.format("Field '%s' is of unsupported type '%s'.",
                                                field.getName(), nonNullableSchema.getDisplayName());
            collector.addFailure(errorMessage, String.format("Supported types are: %s.", supportedTypes))
              .withOutputSchemaField(field.getName());
          }
        }
      }
    }
  }

  /**
   * @return the schema of the dataset
   */
  @Nullable
  public Schema getSchema(FailureCollector collector) {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null).withConfigProperty(SCHEMA);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  public Map<String, String> getColumnMappings() {
    return Strings.isNullOrEmpty(columnMappings) ? Collections.emptyMap() :
      ConfigUtil.parseKeyValueConfig(columnMappings, ",", "=");
  }

  public Map<String, String> getBigtableOptions() {
    return Strings.isNullOrEmpty(bigtableOptions) ? Collections.emptyMap() :
      ConfigUtil.parseKeyValueConfig(bigtableOptions, ",", "=");
  }

  public ErrorHandling getErrorHandling() {
    return Objects.requireNonNull(ErrorHandling.fromDisplayName(onError));
  }

  public boolean connectionParamsConfigured() {
    return !containsMacro(INSTANCE) && Strings.isNullOrEmpty(instance)
      && !containsMacro(NAME_PROJECT) && Strings.isNullOrEmpty(project)
      && !containsMacro(TABLE) && Strings.isNullOrEmpty(table)
      && !containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH);
  }

  public List<HBaseColumn> getRequestedColumns(FailureCollector collector) {
    List<HBaseColumn> columns = new ArrayList<>();

    for (Map.Entry<String, String> entry : getColumnMappings().entrySet()) {
      try {
        columns.add(HBaseColumn.fromFullName(entry.getKey()));
      } catch (IllegalArgumentException e) {
        String errorMessage = String.format("Invalid column in mapping '%s'. Reason: %s",
                                            entry.getKey(), e.getMessage());
        collector.addFailure(errorMessage, "Specify valid column mappings.")
          .withConfigElement(COLUMN_MAPPINGS, ConfigUtil.getKVPair(entry.getKey(), entry.getValue(), "="));
      }
    }
    return columns;
  }
}
