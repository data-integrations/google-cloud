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
package co.cask.gcp.datastore.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.validation.InvalidConfigPropertyException;
import co.cask.gcp.common.GCPReferenceSourceConfig;
import co.cask.gcp.datastore.source.util.DatastoreSourceConstants;
import co.cask.gcp.datastore.source.util.SourceKeyType;
import co.cask.gcp.datastore.util.DatastorePropertyUtil;
import co.cask.gcp.datastore.util.DatastoreUtil;
import co.cask.hydrator.common.KeyValueListParser;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.PathElement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.client.DatastoreHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

  public Schema getSchema() {
    try {
      return schema == null ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      throw new InvalidConfigPropertyException("Unable to parse output schema: " +
                                                 schema, e, DatastoreSourceConstants.PROPERTY_SCHEMA);
    }
  }

  public String getNamespace() {
    return DatastorePropertyUtil.getNamespace(namespace);
  }

  public String getKind() {
    return kind;
  }

  public List<PathElement> getAncestor() {
    return DatastorePropertyUtil.parseKeyLiteral(ancestor);
  }

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

  public SourceKeyType getKeyType() {
    return SourceKeyType.fromValue(keyType)
      .orElseThrow(() -> new InvalidConfigPropertyException("Unsupported key type value: " + keyType,
                                                            DatastoreSourceConstants.PROPERTY_KEY_TYPE));
  }

  public boolean isIncludeKey() {
    return SourceKeyType.NONE != getKeyType();
  }

  public String getKeyAlias() {
    return DatastorePropertyUtil.getKeyAlias(keyAlias);
  }

  @Override
  public void validate() {
    super.validate();
    validateDatastoreConnection();
    validateKind();
    validateAncestor();
    validateNumSplits();

    if (containsMacro(DatastoreSourceConstants.PROPERTY_SCHEMA)) {
      return;
    }

    Schema schema = getSchema();
    if (schema != null) {
      validateSchema(schema);
      validateFilters(schema);
      validateKeyType(schema);
    }
  }

  @VisibleForTesting
  void validateDatastoreConnection() {
    if (containsMacro(DatastoreSourceConstants.PROPERTY_SERVICE_FILE_PATH)
      || containsMacro(DatastoreSourceConstants.PROPERTY_PROJECT)) {
      return;
    }
    DatastoreUtil.getDatastore(getServiceAccountFilePath(), getProject());
    DatastoreUtil.getDatastoreV1(getServiceAccountFilePath(), getProject());
  }

  private void validateKind() {
    if (containsMacro(DatastoreSourceConstants.PROPERTY_KIND)) {
      return;
    }

    if (Strings.isNullOrEmpty(kind)) {
      throw new InvalidConfigPropertyException("Kind cannot be null or empty", DatastoreSourceConstants.PROPERTY_KIND);
    }
  }

  private void validateAncestor() {
    if (!containsMacro(DatastoreSourceConstants.PROPERTY_ANCESTOR)) {
      getAncestor();
    }
  }

  private void validateNumSplits() {
    if (containsMacro(DatastoreSourceConstants.PROPERTY_NUM_SPLITS)) {
      return;
    }

    if (numSplits < 1) {
      throw new InvalidConfigPropertyException("Number of splits must be greater than 0",
                                               DatastoreSourceConstants.PROPERTY_NUM_SPLITS);
    }
  }

  private void validateSchema(Schema schema) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new InvalidConfigPropertyException("Source schema must contain at least one field",
                                               DatastoreSourceConstants.PROPERTY_SCHEMA);
    }

    fields.forEach(f -> validateFieldSchema(f.getName(), f.getSchema()));
  }

  /**
   * Validates given field schema to be compliant with Datastore types.
   * Will throw {@link InvalidConfigPropertyException} if schema contains unsupported type.
   *
   * @param fieldName field name
   * @param fieldSchema schema for CDAP field
   */
  private void validateFieldSchema(String fieldName, Schema fieldSchema) {
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        // timestamps in CDAP are represented as LONG with TIMESTAMP_MICROS logical type
        case TIMESTAMP_MICROS:
          break;
        default:
          throw new InvalidConfigPropertyException(
            String.format("Field '%s' is of unsupported type '%s'", fieldName, logicalType.getToken()),
            DatastoreSourceConstants.PROPERTY_SCHEMA);
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
        validateSchema(fieldSchema);
        return;
      case ARRAY:
        Schema componentSchema = Objects.requireNonNull(fieldSchema.getComponentSchema(),
          String.format("Field '%s' has no schema for array type", fieldName));
        if (Schema.Type.ARRAY == componentSchema.getType()) {
          throw new IllegalArgumentException(
            String.format("Field '%s' is of unsupported type '%s of %s'", fieldName, fieldSchema.getType(),
                          componentSchema.getType()));
        }
        validateFieldSchema(fieldName, componentSchema);
        return;
      case UNION:
        fieldSchema.getUnionSchemas().forEach(unionSchema -> validateFieldSchema(fieldName, unionSchema));
        return;
      default:
        throw new InvalidConfigPropertyException(
          String.format("Field '%s' is of unsupported type '%s'", fieldName, fieldSchema.getType()),
          DatastoreSourceConstants.PROPERTY_SCHEMA);
    }
  }

  private void validateFilters(Schema schema) {
    if (containsMacro(DatastoreSourceConstants.PROPERTY_FILTERS)) {
      return;
    }

    List<String> missingProperties = getFilters().keySet().stream()
      .filter(k -> schema.getField(k) == null)
      .collect(Collectors.toList());

    if (!missingProperties.isEmpty()) {
      throw new InvalidConfigPropertyException("The following properties are missing in the schema: "
                                                 + missingProperties, DatastoreSourceConstants.PROPERTY_FILTERS);
    }
  }

  /**
   * Validates if key alias column is present in the schema and its type is {@link Schema.Type#STRING}.
   *
   * @param schema CDAP schema
   */
  private void validateKeyType(Schema schema) {
    if (containsMacro(DatastoreSourceConstants.PROPERTY_KEY_TYPE)
      || containsMacro(DatastoreSourceConstants.PROPERTY_KEY_ALIAS)) {
      return;
    }

    if (isIncludeKey()) {
      String key = getKeyAlias();
      Schema.Field field = schema.getField(key);
      if (field == null) {
        throw new InvalidConfigPropertyException(
          String.format("Key field '%s' must exist in the schema", key),
          DatastoreSourceConstants.PROPERTY_KEY_ALIAS);
      }

      Schema fieldSchema = field.getSchema();
      Schema.Type type = fieldSchema.getType();
      if (Schema.Type.STRING != type) {
        String foundType = fieldSchema.isNullable()
          ? "nullable " + fieldSchema.getNonNullable().getType().name()
          : type.name();
        throw new InvalidConfigPropertyException(
          String.format("Key field '%s' type must be non-nullable STRING, not '%s'", key, foundType),
          DatastoreSourceConstants.PROPERTY_KEY_ALIAS);
      }
    }
  }

  /**
   * Constructs protobuf query instance which will be used for query splitting.
   * Adds ancestor and property filters if present in the given configuration.
   *
   * @return protobuf query instance
   */
  public com.google.datastore.v1.Query constructPbQuery() {
    com.google.datastore.v1.Query.Builder builder = com.google.datastore.v1.Query.newBuilder()
      .addKind(KindExpression.newBuilder()
                 .setName(getKind()));

    List<Filter> filters = getFilters().entrySet().stream()
      .map(e -> DatastoreHelper.makeFilter(e.getKey(), PropertyFilter.Operator.EQUAL,
                                           constructFilterValue(e.getKey(), e.getValue(), getSchema()))
        .build())
      .collect(Collectors.toList());

    List<PathElement> ancestors = getAncestor();
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
      switch (logicalType) {
        case TIMESTAMP_MICROS:
          Timestamp timestamp = Timestamp.parseTimestamp(value);
          return com.google.datastore.v1.Value.newBuilder()
            .setTimestampValue(timestamp.toProto())
            .build();
        default:
          throw new IllegalStateException(
            String.format("Filter field '%s' is of unsupported type '%s'", name, logicalType.getToken()));
      }
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
}
