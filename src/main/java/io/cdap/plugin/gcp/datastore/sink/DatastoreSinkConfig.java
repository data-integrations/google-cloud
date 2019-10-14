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
package io.cdap.plugin.gcp.datastore.sink;

import com.google.cloud.datastore.PathElement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;
import io.cdap.plugin.gcp.datastore.exception.DatastoreInitializationException;
import io.cdap.plugin.gcp.datastore.sink.util.DatastoreSinkConstants;
import io.cdap.plugin.gcp.datastore.sink.util.IndexStrategy;
import io.cdap.plugin.gcp.datastore.sink.util.SinkKeyType;
import io.cdap.plugin.gcp.datastore.source.DatastoreSourceConfig;
import io.cdap.plugin.gcp.datastore.util.DatastorePropertyUtil;
import io.cdap.plugin.gcp.datastore.util.DatastoreUtil;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * This class {@link DatastoreSinkConfig} provides all the configuration required for
 * configuring the {@link DatastoreSink} plugin.
 */
public class DatastoreSinkConfig extends GCPReferenceSinkConfig {

  @Name(DatastoreSinkConstants.PROPERTY_NAMESPACE)
  @Macro
  @Nullable
  @Description("Namespace of the entities to write. A namespace partitions entities into a subset of Cloud Datastore."
    + "If no value is provided, the [default] namespace will be used.")
  private String namespace;

  @Name(DatastoreSinkConstants.PROPERTY_KIND)
  @Macro
  @Description("Kind of entities to write. Kinds are used to categorize entities in Cloud Datastore. "
    + "A kind is equivalent to the relational database table notion.")
  private String kind;

  @Name(DatastoreSinkConstants.PROPERTY_KEY_TYPE)
  @Macro
  @Description("Type of key assigned to entities written to the Datastore. The type can be one of four values: "
    + "`Auto-generated key` - key will be generated Cloud Datastore as a Numeric ID, `Custom name` - key "
    + "will be provided as a field in the input records. The key field must not be nullable and must be "
    + "of type STRING, INT or LONG, `Key literal` - key will be provided as a field in the input records in "
    + "key literal format. The key field type must be a non-nullable string and the value must be in key literal "
    + "format, `URL-safe key` - key will be provided as a field in the input records in encoded URL form. The "
    + "key field type must be a non-nullable string and the value must be a URL-safe string.")
  private String keyType;

  @Name(DatastoreSinkConstants.PROPERTY_KEY_ALIAS)
  @Macro
  @Nullable
  @Description("The field that will be used as the entity key when writing to Cloud Datastore. This must be provided "
    + "when the Key Type is not auto generated.")
  private String keyAlias;

  @Name(DatastoreSinkConstants.PROPERTY_ANCESTOR)
  @Macro
  @Nullable
  @Description("Ancestor identifies the common root entity in which the entities are grouped. "
    + "An ancestor must be specified in key literal format: key(<kind>, <identifier>, <kind>, <identifier>, [...]). "
    + "Example: `key(kind_1, 'stringId', kind_2, 100)`")
  private String ancestor;

  @Name(DatastoreSinkConstants.PROPERTY_INDEX_STRATEGY)
  @Macro
  @Description("Defines which fields will be indexed in Cloud Datastore. "
    + "Can be one of three options: `All` - all fields will be indexed, `None` - none of fields will be "
    + "indexed, `Custom` - indexed fields will be provided in `Indexed Properties`.")
  private String indexStrategy;

  @Name(DatastoreSinkConstants.PROPERTY_INDEXED_PROPERTIES)
  @Macro
  @Nullable
  @Description("Fields to index in Cloud Datastore. A value must be provided if the Index Strategy is Custom, "
    + "otherwise it is ignored.")
  private String indexedProperties;

  @Name(DatastoreSinkConstants.PROPERTY_BATCH_SIZE)
  @Macro
  @Description("Maximum number of entities that can be passed in one batch to a Commit operation. "
    + "The minimum value is 1 and maximum value is 500")
  private int batchSize;

  public DatastoreSinkConfig() {
    // needed for initialization
  }

  @VisibleForTesting
  public DatastoreSinkConfig(String referenceName,
                             String project,
                             String serviceFilePath,
                             @Nullable String namespace,
                             String kind,
                             String keyType,
                             @Nullable String keyAlias,
                             @Nullable String ancestor,
                             String indexStrategy,
                             int batchSize,
                             @Nullable String indexedProperties) {
    this.referenceName = referenceName;
    this.project = project;
    this.serviceFilePath = serviceFilePath;
    this.namespace = namespace;
    this.kind = kind;
    this.indexStrategy = indexStrategy;
    this.indexedProperties = indexedProperties;
    this.keyType = keyType;
    this.keyAlias = keyAlias;
    this.ancestor = ancestor;
    this.batchSize = batchSize;
  }

  public String getNamespace() {
    return DatastorePropertyUtil.getNamespace(namespace);
  }

  public String getKind() {
    return kind;
  }

  public SinkKeyType getKeyType(FailureCollector collector) {
    Optional<SinkKeyType> sinkKeyType = SinkKeyType.fromValue(keyType);
    if (sinkKeyType.isPresent()) {
      return sinkKeyType.get();
    }
    collector.addFailure("Unsupported key type value: " + keyType,
                         String.format("Supported types are: %s", SinkKeyType.getSupportedTypes()))
      .withConfigProperty(DatastoreSinkConstants.PROPERTY_KEY_TYPE);
    throw collector.getOrThrowException();
  }

  public String getKeyAlias() {
    return DatastorePropertyUtil.getKeyAlias(keyAlias);
  }

  public List<PathElement> getAncestor(FailureCollector collector) {
    try {
      return DatastorePropertyUtil.parseKeyLiteral(ancestor);
    } catch (IllegalArgumentException e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(DatastoreSinkConstants.PROPERTY_ANCESTOR);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  public IndexStrategy getIndexStrategy(FailureCollector collector) {
    Optional<IndexStrategy> indexStrategy = IndexStrategy.fromValue(this.indexStrategy);
    if (indexStrategy.isPresent()) {
      return indexStrategy.get();
    }
    collector.addFailure("Unsupported index strategy value: " + this.indexStrategy,
                         String.format("Supported index strategies are: %s", IndexStrategy.getSupportedStrategies()))
      .withConfigProperty(DatastoreSinkConstants.PROPERTY_INDEX_STRATEGY);

    throw collector.getOrThrowException();
  }

  public Set<String> getIndexedProperties() {
    if (Strings.isNullOrEmpty(indexedProperties)) {
      return Collections.emptySet();
    }
    return Stream.of(indexedProperties.split(","))
      .map(String::trim)
      .filter(name -> !name.isEmpty())
      .collect(Collectors.toSet());
  }

  public int getBatchSize() {
    return batchSize;
  }

  public boolean shouldUseAutoGeneratedKey(FailureCollector collector) {
    return getKeyType(collector) == SinkKeyType.AUTO_GENERATED_KEY;
  }

  public void validate(@Nullable Schema schema, FailureCollector collector) {
    super.validate(collector);
    validateKind(collector);
    validateAncestors(collector);
    validateBatchSize(collector);
    validateDatastoreConnection(collector);

    if (schema != null) {
      validateSchema(schema, collector);
      validateKeyType(schema, collector);
      validateIndexStrategy(schema, collector);
    }
  }

  private void validateSchema(Schema schema, FailureCollector collector) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      collector.addFailure("Sink schema must contain at least one field", null);
    } else {
      fields.forEach(f -> validateSinkFieldSchema(f.getName(), f.getSchema(), collector));
    }
  }

  /**
   * Validates given field schema to be complaint with Datastore types.
   * Will throw {@link IllegalArgumentException} if schema contains unsupported type.
   *
   * @param fieldName field name
   * @param fieldSchema schema for CDAP field
   * @param collector failure collector
   */
  private void validateSinkFieldSchema(String fieldName, Schema fieldSchema, FailureCollector collector) {
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        // timestamps in CDAP are represented as LONG with TIMESTAMP_MICROS logical type
        case TIMESTAMP_MICROS:
        case TIMESTAMP_MILLIS:
          break;
        default:
          collector.addFailure(String.format("Field '%s' is of unsupported type '%s'",
                                             fieldName, fieldSchema.getDisplayName()),
                               "Supported types are: string, double, boolean, bytes, int, float, long, record, " +
                                 "array, union and timestamp.").withInputSchemaField(fieldName);
      }
      return;
    }

    switch (fieldSchema.getType()) {
      case STRING:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
      case INT:
      case FLOAT:
      case LONG:
      case NULL:
        return;
      case RECORD:
        validateSchema(fieldSchema, collector);
        return;
      case ARRAY:
        if (fieldSchema.getComponentSchema() == null) {
          collector.addFailure(String.format("Field '%s' has no schema for array type", fieldName),
                               "Ensure array component has schema.").withInputSchemaField(fieldName);
          return;
        }
        Schema componentSchema = fieldSchema.getComponentSchema();
        if (Schema.Type.ARRAY == componentSchema.getType()) {
          collector.addFailure(String.format("Field '%s' is of unsupported type array of array.", fieldName),
                               "Ensure the field has valid type.")
            .withInputSchemaField(fieldName);
          return;
        }
        validateSinkFieldSchema(fieldName, componentSchema, collector);
        return;
      case UNION:
        fieldSchema.getUnionSchemas().forEach(unionSchema ->
                                                validateSinkFieldSchema(fieldName, unionSchema, collector));
        return;
      default:
        collector.addFailure(String.format("Field '%s' is of unsupported type '%s'",
                                           fieldName, fieldSchema.getDisplayName()),
                             "Supported types are: string, double, boolean, bytes, long, record, " +
                               "array, union and timestamp.")
          .withInputSchemaField(fieldName);
    }
  }

  private void validateIndexStrategy(Schema schema, FailureCollector collector) {
    if (containsMacro(DatastoreSinkConstants.PROPERTY_INDEX_STRATEGY)) {
      return;
    }

    if (getIndexStrategy(collector) == IndexStrategy.CUSTOM) {
      validateIndexedProperties(schema, collector);
    }
  }

  @VisibleForTesting
  void validateDatastoreConnection(FailureCollector collector) {
    if (!shouldConnect()) {
      return;
    }
    try {
      DatastoreUtil.getDatastore(getServiceAccountFilePath(), getProject());
    } catch (DatastoreInitializationException e) {
      collector.addFailure(e.getMessage(), "Ensure properties like project, service account file path are correct.")
        .withConfigProperty(DatastoreSinkConstants.PROPERTY_SERVICE_FILE_PATH)
        .withConfigProperty(DatastoreSinkConstants.PROPERTY_PROJECT);
    }
  }

  private void validateKind(FailureCollector collector) {
    if (containsMacro(DatastoreSinkConstants.PROPERTY_KIND)) {
      return;
    }
    if (Strings.isNullOrEmpty(kind)) {
      collector.addFailure("Kind must be specified.", null)
        .withConfigProperty(DatastoreSinkConstants.PROPERTY_KIND);
    }
  }

  /**
   * If key type is not auto-generated, validates if key alias column is present in the schema
   * and its type is {@link Schema.Type#STRING}, {@link Schema.Type#INT} or {@link Schema.Type#LONG}.
   *
   * @param schema CDAP schema
   * @param collector failure collector
   */
  private void validateKeyType(Schema schema, FailureCollector collector) {
    if (containsMacro(DatastoreSinkConstants.PROPERTY_KEY_TYPE) || shouldUseAutoGeneratedKey(collector)) {
      return;
    }

    SinkKeyType keyType = getKeyType(collector);
    Schema.Field field = schema.getField(keyAlias);
    if (field == null) {
      collector.addFailure(String.format("Key field '%s' does not exist in the schema", keyAlias),
                           "Change the Key field to be one of the schema fields.")
        .withConfigProperty(DatastoreSinkConstants.PROPERTY_KEY_ALIAS);
      return;
    }

    Schema fieldSchema = field.getSchema();
    Schema.Type type = fieldSchema.getType();
    if (Schema.Type.STRING != type && Schema.Type.LONG != type && Schema.Type.INT != type) {
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      collector.addFailure(
        String.format("Key field '%s' is of unsupported type '%s'", keyAlias, fieldSchema.getDisplayName()),
        "Ensure the type is non-nullable string, int or long.")
        .withConfigProperty(DatastoreSinkConstants.PROPERTY_KEY_ALIAS).withInputSchemaField(keyAlias);
    } else if ((Schema.Type.LONG == type || Schema.Type.INT == type) && keyType != SinkKeyType.CUSTOM_NAME) {
      collector.addFailure(
        String.format("Incorrect Key field '%s' type defined '%s'.", keyAlias, fieldSchema.getDisplayName()),
        String.format("'%s' type supported only by Key type '%s'",
                      type, SinkKeyType.CUSTOM_NAME.getValue()))
        .withConfigProperty(DatastoreSinkConstants.PROPERTY_KEY_ALIAS).withInputSchemaField(keyAlias);
    }
  }

  private void validateIndexedProperties(Schema schema, FailureCollector collector) {
    if (containsMacro(DatastoreSinkConstants.PROPERTY_INDEXED_PROPERTIES)) {
      return;
    }
    Set<String> indexedProperties = getIndexedProperties();
    if (indexedProperties.isEmpty()) {
      return;
    }
    List<String> missedProperties = indexedProperties.stream()
      .filter(name -> schema.getField(name) == null)
      .collect(Collectors.toList());

    for (String missingProperty : missedProperties) {
      collector.addFailure(String.format("Index Property '%s' does not exist in the input schema.", missingProperty),
                           "Change Index property to be one of the schema fields.")
        .withConfigElement(DatastoreSinkConstants.PROPERTY_INDEXED_PROPERTIES, missingProperty);
    }
  }

  private void validateAncestors(FailureCollector collector) {
    if (containsMacro(DatastoreSinkConstants.PROPERTY_ANCESTOR)) {
      return;
    }
    getAncestor(collector);
  }

  private void validateBatchSize(FailureCollector collector) {
    if (containsMacro(DatastoreSinkConstants.PROPERTY_BATCH_SIZE)) {
      return;
    }
    if (batchSize < 1 || batchSize > DatastoreSinkConstants.MAX_BATCH_SIZE) {
      collector.addFailure(String.format("Invalid Datastore batch size '%d'.", batchSize),
                           String.format("Ensure the batch size is at least 1 or at most '%d'",
                                         DatastoreSinkConstants.MAX_BATCH_SIZE))
        .withConfigProperty(DatastoreSinkConstants.PROPERTY_BATCH_SIZE);
    }
  }

  /**
   * Returns true if datastore can be connected to
   */
  public boolean shouldConnect() {
    return !containsMacro(DatastoreSourceConfig.NAME_SERVICE_ACCOUNT_FILE_PATH) &&
      !containsMacro(DatastoreSourceConfig.NAME_PROJECT) &&
      tryGetProject() != null &&
      !autoServiceAccountUnavailable();
  }
}
