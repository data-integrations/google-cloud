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
package io.cdap.plugin.gcp.datastore.source;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.PathElement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;
import io.cdap.plugin.gcp.datastore.exception.DatastoreInitializationException;
import io.cdap.plugin.gcp.datastore.source.util.DatastoreSourceConstants;
import io.cdap.plugin.gcp.datastore.source.util.SourceKeyType;
import io.cdap.plugin.gcp.datastore.util.DatastorePropertyUtil;
import io.cdap.plugin.gcp.datastore.util.DatastoreUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * This class {@link DatastoreSourceConfig} provides all the configuration required for
 * configuring the {@link DatastoreSource} plugin.
 */
public class DatastoreSourceConfig extends GCPReferenceSourceConfig {
  private static final KeyValueListParser KV_PARSER = new KeyValueListParser(";", "\\|");

  @Name(DatastoreSourceConstants.PROPERTY_NAMESPACE)
  @Macro
  @Nullable
  @Description("Namespace of the entities to read. A namespace partitions entities into a subset of Cloud Datastore. "
    + "If no value is provided, the `[default]` namespace will be used.")
  private String namespace;

  @Name(DatastoreSourceConstants.PROPERTY_KIND)
  @Macro
  @Description("Kind of entities to read. Kinds are used to categorize entities in Cloud Datastore. "
    + "A kind is equivalent to the relational database table notion.")
  private String kind;

  @Name(DatastoreSourceConstants.PROPERTY_ANCESTOR)
  @Macro
  @Nullable
  @Description("Ancestor of entities to read. An ancestor identifies the common parent entity "
    + "that all the child entities share. The value must be provided in key literal format: "
    + "key(<kind>, <identifier>, <kind>, <identifier>, [...]). "
    + "For example: `key(kind_1, 'stringId', kind_2, 100)`")
  private String ancestor;

  @Name(DatastoreSourceConstants.PROPERTY_FILTERS)
  @Macro
  @Nullable
  @Description("List of filters to apply when reading entities from Cloud Datastore. "
    + "Only entities that satisfy all the filters will be read. "
    + "The filter key corresponds to a field in the schema. "
    + "The field type must be STRING, LONG, DOUBLE, BOOLEAN, or TIMESTAMP. "
    + "The filter value indicates what value that field must have in order to be read. "
    + "If no value is provided, it means the value must be null in order to be read. "
    + "TIMESTAMP string should be in the RFC 3339 format without the timezone offset (always ends in Z). "
    + "Expected pattern: `yyyy-MM-dd'T'HH:mm:ssX`, for example: `2011-10-02T13:12:55Z`.")
  private String filters;

  @Name(DatastoreSourceConstants.PROPERTY_NUM_SPLITS)
  @Macro
  @Description("Desired number of splits to divide the query into when reading from Cloud Datastore. "
    + "Fewer splits may be created if the query cannot be divided into the desired number of splits.")
  private int numSplits;

  @Name(DatastoreSourceConstants.PROPERTY_KEY_TYPE)
  @Macro
  @Description("Type of entity key read from the Cloud Datastore. The type can be one of three values: "
    + "`None` - key will not be included, `Key literal` - key will be included "
    + "in Cloud Datastore key literal format including complete path with ancestors, `URL-safe key` - key "
    + "will be included in the encoded form that can be used as part of a URL. "
    + "Note, if `Key literal` or `URL-safe key` is selected, default key name (`__key__`) or its alias must be present "
    + "in the schema with non-nullable STRING type.")
  private String keyType;

  @Name(DatastoreSourceConstants.PROPERTY_KEY_ALIAS)
  @Macro
  @Nullable
  @Description("Name of the field to set as the key field. This value is ignored if the `Key Type` is set to `None`. "
    + "If no value is provided, `__key__` is used.")
  private String keyAlias;

  @Name(DatastoreSourceConstants.PROPERTY_SCHEMA)
  @Macro
  @Nullable
  @Description("Schema of the data to read. Can be imported or fetched by clicking the `Get Schema` button.")
  private String schema;

  public DatastoreSourceConfig() {
    // needed for initialization
  }

  @VisibleForTesting
  DatastoreSourceConfig(String referenceName,
                        String project,
                        String serviceFilePath,
                        @Nullable String namespace,
                        String kind,
                        @Nullable String ancestor,
                        @Nullable String filters,
                        int numSplits,
                        String keyType,
                        @Nullable String keyAlias,
                        String schema) {
    this.referenceName = referenceName;
    this.project = project;
    this.serviceFilePath = serviceFilePath;
    this.namespace = namespace;
    this.kind = kind;
    this.ancestor = ancestor;
    this.filters = filters;
    this.numSplits = numSplits;
    this.keyType = keyType;
    this.keyAlias = keyAlias;
    this.schema = schema;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public Schema getSchema(FailureCollector collector) {
    if (Strings.isNullOrEmpty(schema)) {
      return null;
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null)
        .withConfigProperty(DatastoreSourceConstants.PROPERTY_SCHEMA);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  public String getNamespace() {
    return DatastorePropertyUtil.getNamespace(namespace);
  }

  public String getKind() {
    return kind;
  }

  public List<PathElement> getAncestor(FailureCollector collector) {
    try {
      return DatastorePropertyUtil.parseKeyLiteral(ancestor);
    } catch (IllegalArgumentException e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(DatastoreSourceConstants.PROPERTY_ANCESTOR);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  /**
   * Returns a map of filters.
   * @throws IllegalArgumentException if kv parse can not parse the filter property
   */
  public Map<String, String> getFilters() {
    if (Strings.isNullOrEmpty(filters)) {
      return Collections.emptyMap();
    }
    return StreamSupport.stream(KV_PARSER.parse(filters).spliterator(), false)
      .collect(Collectors.toMap(
        KeyValue::getKey,
        KeyValue::getValue,
        (o, n) -> n,
        LinkedHashMap::new));
  }

  public int getNumSplits() {
    return numSplits;
  }

  public SourceKeyType getKeyType(FailureCollector collector) {
    Optional<SourceKeyType> sourceKeyType = SourceKeyType.fromValue(keyType);
    if (sourceKeyType.isPresent()) {
      return sourceKeyType.get();
    }
    collector.addFailure("Unsupported key type value: " + keyType,
                         String.format("Supported key types are: %s", SourceKeyType.getSupportedTypes()))
      .withConfigProperty(DatastoreSourceConstants.PROPERTY_KEY_TYPE);
    throw collector.getOrThrowException();
  }

  public boolean isIncludeKey(FailureCollector collector) {
    return SourceKeyType.NONE != getKeyType(collector);
  }

  public String getKeyAlias() {
    return DatastorePropertyUtil.getKeyAlias(keyAlias);
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);
    validateDatastoreConnection(collector);
    validateKind(collector);
    validateAncestor(collector);
    validateNumSplits(collector);

    if (containsMacro(DatastoreSourceConstants.PROPERTY_SCHEMA)) {
      return;
    }

    Schema schema = getSchema(collector);
    if (schema != null) {
      validateSchema(schema, collector);
      validateFilters(schema, collector);
      validateKeyType(schema, collector);
    }
  }

  @VisibleForTesting
  void validateDatastoreConnection(FailureCollector collector) {
    if (!shouldConnect()) {
      return;
    }
    try {
      DatastoreUtil.getDatastore(getServiceAccountFilePath(), getProject());
      DatastoreUtil.getDatastoreV1(getServiceAccountFilePath(), getProject());
    } catch (DatastoreInitializationException e) {
      collector.addFailure(e.getMessage(), "Ensure properties like project, service account file path are correct.")
        .withConfigProperty(DatastoreSourceConstants.PROPERTY_SERVICE_FILE_PATH)
        .withConfigProperty(DatastoreSourceConstants.PROPERTY_PROJECT);
    }
  }

  private void validateKind(FailureCollector collector) {
    if (containsMacro(DatastoreSourceConstants.PROPERTY_KIND)) {
      return;
    }

    if (Strings.isNullOrEmpty(kind)) {
      collector.addFailure("Kind must be specified.", null)
        .withConfigProperty(DatastoreSourceConstants.PROPERTY_KIND);
    }
  }

  private void validateAncestor(FailureCollector collector) {
    if (!containsMacro(DatastoreSourceConstants.PROPERTY_ANCESTOR)) {
      getAncestor(collector);
    }
  }

  private void validateNumSplits(FailureCollector collector) {
    if (containsMacro(DatastoreSourceConstants.PROPERTY_NUM_SPLITS)) {
      return;
    }

    if (numSplits < 1) {
      collector.addFailure("Number of splits must be greater than 0", null)
        .withConfigProperty(DatastoreSourceConstants.PROPERTY_NUM_SPLITS);
    }
  }

  private void validateSchema(Schema schema, FailureCollector collector) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      collector.addFailure("Source schema must contain at least one field", null)
        .withConfigProperty(DatastoreSourceConstants.PROPERTY_SCHEMA);
    } else {
      fields.forEach(f -> validateFieldSchema(f.getName(), f.getSchema(), collector));
    }
  }

  /**
   * Validates given field schema to be compliant with Datastore types.
   *
   * @param fieldName field name
   * @param fieldSchema schema for CDAP field
   * @param collector failure collector to collect failures if schema contains unsupported type.
   */
  private void validateFieldSchema(String fieldName, Schema fieldSchema, FailureCollector collector) {
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      // timestamps in CDAP are represented as LONG with TIMESTAMP_MICROS logical type
      if (logicalType != Schema.LogicalType.TIMESTAMP_MICROS) {
        collector.addFailure(String.format("Field '%s' is of unsupported type '%s'",
                                           fieldName, fieldSchema.getDisplayName()),
                             "Supported types are: string, double, boolean, bytes, long, record, " +
                               "array, union and timestamp.")
          .withOutputSchemaField(fieldName);
        return;
      }
    }

    switch (fieldSchema.getType()) {
      case STRING:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
      case LONG:
      case NULL:
        return;
      case RECORD:
        validateSchema(fieldSchema, collector);
        return;
      case ARRAY:
        if (fieldSchema.getComponentSchema() == null) {
          collector.addFailure(String.format("Field '%s' has no schema for array type", fieldName),
                               "Ensure array component has schema.").withOutputSchemaField(fieldName);
          return;
        }

        Schema componentSchema = fieldSchema.getComponentSchema();
        if (Schema.Type.ARRAY == componentSchema.getType()) {
          collector.addFailure(String.format("Field '%s' is of unsupported type array of array.", fieldName),
                               "Ensure the field has valid type.")
            .withOutputSchemaField(fieldName);
          return;
        }
        validateFieldSchema(fieldName, componentSchema, collector);

        return;
      case UNION:
        fieldSchema.getUnionSchemas().forEach(unionSchema ->
                                                validateFieldSchema(fieldName, unionSchema, collector));
        return;
      default:
        collector.addFailure(String.format("Field '%s' is of unsupported type '%s'",
                                           fieldName, fieldSchema.getDisplayName()),
                             "Supported types are: string, double, boolean, bytes, long, record, " +
                               "array, union and timestamp.")
          .withOutputSchemaField(fieldName);
    }
  }

  private void validateFilters(Schema schema, FailureCollector collector) {
    if (containsMacro(DatastoreSourceConstants.PROPERTY_FILTERS)) {
      return;
    }

    try {
      Map<String, String> filters = getFilters();
      List<String> missingProperties = filters.keySet().stream()
        .filter(k -> schema.getField(k) == null)
        .collect(Collectors.toList());

      for (String missingProperty : missingProperties) {
        collector.addFailure(String.format("Property '%s' does not exist in the schema.", missingProperty),
                             "Change Property to be one of the schema fields.")
          .withConfigElement(DatastoreSourceConstants.PROPERTY_FILTERS,
                             missingProperty + "|" + filters.get(missingProperty));
      }
    } catch (IllegalArgumentException e) {
      // IllegalArgumentException is thrown from getFilters method.
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(DatastoreSourceConstants.PROPERTY_FILTERS);
    }
  }

  /**
   * Validates if key alias column is present in the schema and its type is {@link Schema.Type#STRING}.
   *
   * @param schema CDAP schema
   */
  private void validateKeyType(Schema schema, FailureCollector collector) {
    if (containsMacro(DatastoreSourceConstants.PROPERTY_KEY_TYPE)
      || containsMacro(DatastoreSourceConstants.PROPERTY_KEY_ALIAS)) {
      return;
    }

    if (isIncludeKey(collector)) {
      String key = getKeyAlias();
      Schema.Field field = schema.getField(key);
      if (field == null) {
        collector.addFailure(String.format("Key field '%s' does not exist in the schema.", key),
                             "Change the Key field to be one of the schema fields.")
          .withConfigProperty(DatastoreSourceConstants.PROPERTY_KEY_ALIAS);
        return;
      }

      Schema fieldSchema = field.getSchema();
      Schema.Type type = fieldSchema.getType();
      if (Schema.Type.STRING != type) {
        fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;

        collector.addFailure(String.format("Key field '%s' is of unsupported type '%s'", key,
                                           fieldSchema.getDisplayName()),
                             "Ensure the type is non-nullable String.")
          .withConfigProperty(DatastoreSourceConstants.PROPERTY_KEY_ALIAS).withOutputSchemaField(field.getName());
      }
    }
  }

  /**
   * Constructs protobuf query instance which will be used for query splitting.
   * Adds ancestor and property filters if present in the given configuration.
   *
   * @param collector failure collector
   * @return protobuf query instance
   */
  public com.google.datastore.v1.Query constructPbQuery(FailureCollector collector) {
    com.google.datastore.v1.Query.Builder builder = com.google.datastore.v1.Query.newBuilder()
      .addKind(KindExpression.newBuilder()
                 .setName(getKind()));

    List<Filter> filters = getFilters().entrySet().stream()
      .map(e -> DatastoreHelper.makeFilter(e.getKey(), PropertyFilter.Operator.EQUAL,
                                           constructFilterValue(e.getKey(), e.getValue(), getSchema(collector)))
        .build())
      .collect(Collectors.toList());

    List<PathElement> ancestors = getAncestor(collector);
    if (!ancestors.isEmpty()) {
      filters.add(DatastoreHelper.makeAncestorFilter(constructKey(ancestors, getProject(), getNamespace())).build());
    }

    if (!filters.isEmpty()) {
      builder.setFilter(DatastoreHelper.makeAndFilter(filters));
    }

    return builder.build();
  }

  /**
   * Constructs Datastore protobuf key instance based on given list of path elements
   * and Datastore configuration.
   *
   * @param pathElements list of path elements
   * @param project project ID
   * @param namespace namespace name
   * @return Datastore protobuf key instance
   */
  private com.google.datastore.v1.Key constructKey(List<PathElement> pathElements, String project, String namespace) {
    Object[] elements = pathElements.stream()
      .flatMap(pathElement -> Stream.of(pathElement.getKind(), pathElement.getNameOrId()))
      .toArray();

    return DatastoreHelper.makeKey(elements)
      .setPartitionId(PartitionId.newBuilder()
                        .setProjectId(project)
                        .setNamespaceId(namespace)
                        .build())
      .build();
  }

  /**
   * Transforms given value into value holder corresponding to the given field schema type.
   * If given value is empty, creates null value holder (`is null` clause).
   *
   * @param name field name
   * @param value filter value in string representation
   * @param schema field schema
   * @return protobuf value for filter
   */
  private com.google.datastore.v1.Value constructFilterValue(String name, @Nullable String value, Schema schema) {
    Schema.Field field = Objects.requireNonNull(schema.getField(name));
    Schema fieldSchema = field.getSchema();

    if (Strings.isNullOrEmpty(value)) {
      return com.google.datastore.v1.Value.newBuilder()
        .setNullValue(com.google.protobuf.NullValue.NULL_VALUE)
        .build();
    }

    return constructFilterValue(name, fieldSchema, value);
  }

  /**
   * Transforms given value into value holder corresponding to the given field schema type.
   * May call itself recursively of schema is of UNION type.
   *
   * @param name field name
   * @param schema field schema
   * @param value value in string representation
   * @return protobuf value for filter
   */
  private com.google.datastore.v1.Value constructFilterValue(String name, Schema schema, String value) {
    Schema.LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      if (logicalType == Schema.LogicalType.TIMESTAMP_MICROS) {
        Timestamp timestamp = Timestamp.parseTimestamp(value);
        return Value.newBuilder()
          .setTimestampValue(timestamp.toProto())
          .build();
      }
      throw new IllegalStateException(
        String.format("Filter field '%s' is of unsupported type '%s'", name, logicalType.getToken()));
    }

    switch (schema.getType()) {
      case STRING:
        return DatastoreHelper.makeValue(value).build();
      case DOUBLE:
        return DatastoreHelper.makeValue(Double.valueOf(value)).build();
      case LONG:
        return DatastoreHelper.makeValue(Long.valueOf(value)).build();
      case BOOLEAN:
        return DatastoreHelper.makeValue(Boolean.valueOf(value)).build();
      case UNION:
        // nullable fields in CDAP are represented as UNION of NULL and FIELD_TYPE
        if (schema.isNullable()) {
          return constructFilterValue(name, schema.getNonNullable(), value);
        }
        throw new IllegalStateException(
          String.format("Filter field '%s' is of unsupported type 'complex UNION'", name));
      default:
        throw new IllegalStateException(
          String.format("Filter field '%s' is of unsupported type '%s'", name, schema.getType()));
    }
  }

  @Override
  public String toString() {
    return "DatastoreSourceConfig{" +
      "referenceName='" + referenceName + '\'' +
      ", project='" + project + '\'' +
      ", serviceFilePath='" + serviceFilePath + '\'' +
      ", namespace='" + namespace + '\'' +
      ", kind='" + kind + '\'' +
      ", ancestor='" + ancestor + '\'' +
      ", filters='" + filters + '\'' +
      ", numSplits=" + numSplits +
      ", keyType='" + keyType + '\'' +
      ", keyAlias='" + keyAlias + '\'' +
      ", schema='" + schema + '\'' +
      "} ";
  }

  /**
   * Returns true if datastore can be connected to or schema is not a macro.
   */
  boolean shouldConnect() {
    return !containsMacro(DatastoreSourceConstants.PROPERTY_SCHEMA) &&
      !containsMacro(DatastoreSourceConfig.NAME_SERVICE_ACCOUNT_FILE_PATH) &&
      !containsMacro(DatastoreSourceConfig.NAME_PROJECT) &&
      tryGetProject() != null &&
      !autoServiceAccountUnavailable();
  }
}
