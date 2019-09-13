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
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;
import io.cdap.plugin.gcp.datastore.sink.util.DatastoreSinkConstants;
import io.cdap.plugin.gcp.datastore.sink.util.IndexStrategy;
import io.cdap.plugin.gcp.datastore.sink.util.SinkKeyType;
import io.cdap.plugin.gcp.datastore.util.DatastorePropertyUtil;
import io.cdap.plugin.gcp.datastore.util.DatastoreUtil;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
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

  public SinkKeyType getKeyType() {
    return SinkKeyType.fromValue(keyType)
      .orElseThrow(() -> new InvalidConfigPropertyException("Unsupported key type value: " + keyType,
                                                            DatastoreSinkConstants.PROPERTY_KEY_TYPE));
  }

  public String getKeyAlias() {
    return DatastorePropertyUtil.getKeyAlias(keyAlias);
  }

  public List<PathElement> getAncestor() {
    return DatastorePropertyUtil.parseKeyLiteral(ancestor);
  }

  public IndexStrategy getIndexStrategy() {
    return IndexStrategy.fromValue(indexStrategy)
      .orElseThrow(() -> new InvalidConfigPropertyException("Unsupported index strategy value: " + indexStrategy,
                                                            DatastoreSinkConstants.PROPERTY_INDEX_STRATEGY));
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

  public boolean shouldUseAutoGeneratedKey() {
    return getKeyType() == SinkKeyType.AUTO_GENERATED_KEY;
  }

  public void validate(@Nullable Schema schema) {
    // TODO: (vinisha) add failure collector
    super.validate(null);
    validateKind();
    validateAncestors();
    validateBatchSize();
    validateDatastoreConnection();

    if (schema != null) {
      validateSchema(schema);
      validateKeyType(schema);
      validateIndexStrategy(schema);
    }
  }

  private void validateSchema(Schema schema) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new IllegalArgumentException("Sink schema must contain at least one field");
    }
    fields.forEach(f -> validateSinkFieldSchema(f.getName(), f.getSchema()));
  }

  /**
   * Validates given field schema to be complaint with Datastore types.
   * Will throw {@link IllegalArgumentException} if schema contains unsupported type.
   *
   * @param fieldName field name
   * @param fieldSchema schema for CDAP field
   */
  private void validateSinkFieldSchema(String fieldName, Schema fieldSchema) {
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        // timestamps in CDAP are represented as LONG with TIMESTAMP_MICROS logical type
        case TIMESTAMP_MICROS:
        case TIMESTAMP_MILLIS:
          break;
        default:
          throw new IllegalArgumentException(
            String.format("Field '%s' is of unsupported type '%s'", fieldName, logicalType.getToken()));
      }
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
        validateSinkFieldSchema(fieldName, componentSchema);
        return;
      case UNION:
        fieldSchema.getUnionSchemas().forEach(unionSchema -> validateSinkFieldSchema(fieldName, unionSchema));
        return;
      default:
        throw new IllegalArgumentException(
          String.format("Field '%s' is of unsupported type '%s'", fieldName, fieldSchema.getType()));
    }
  }

  private void validateIndexStrategy(Schema schema) {
    if (containsMacro(indexStrategy)) {
      return;
    }

    if (getIndexStrategy() == IndexStrategy.CUSTOM) {
      validateIndexedProperties(schema);
    }
  }

  @VisibleForTesting
  void validateDatastoreConnection() {
    if (containsMacro(DatastoreSinkConstants.PROPERTY_SERVICE_FILE_PATH)
      || containsMacro(DatastoreSinkConstants.PROPERTY_PROJECT)) {
      return;
    }
    DatastoreUtil.getDatastore(getServiceAccountFilePath(), getProject());
  }

  private void validateKind() {
    if (containsMacro(DatastoreSinkConstants.PROPERTY_KIND)) {
      return;
    }
    if (Strings.isNullOrEmpty(kind)) {
      throw new InvalidConfigPropertyException("Kind cannot be null or empty", DatastoreSinkConstants.PROPERTY_KIND);
    }
  }

  /**
   * If key type is not auto-generated, validates if key alias column is present in the schema
   * and its type is {@link Schema.Type#STRING}, {@link Schema.Type#INT} or {@link Schema.Type#LONG}.
   *
   * @param schema CDAP schema
   */
  private void validateKeyType(Schema schema) {
    if (containsMacro(DatastoreSinkConstants.PROPERTY_KEY_TYPE) || shouldUseAutoGeneratedKey()) {
      return;
    }

    SinkKeyType keyType = getKeyType();
    Schema.Field field = schema.getField(keyAlias);
    if (field == null) {
      throw new InvalidConfigPropertyException(String.format("Key alias '%s' does not exist in schema.", keyAlias),
                                               DatastoreSinkConstants.PROPERTY_KEY_ALIAS);
    }

    Schema.Type type = field.getSchema().getType();
    if (Schema.Type.STRING != type && Schema.Type.LONG != type && Schema.Type.INT != type) {
      String foundType = field.getSchema().isNullable()
        ? "nullable " + field.getSchema().getNonNullable().getType().name()
        : type.name();
      throw new InvalidConfigPropertyException(
        String.format("Key field '%s' type must be non-nullable STRING, INT or LONG, not '%s'", keyAlias,
                      foundType), DatastoreSinkConstants.PROPERTY_KEY_ALIAS);
    } else if ((Schema.Type.LONG == type || Schema.Type.INT == type) && keyType != SinkKeyType.CUSTOM_NAME) {
      throw new InvalidConfigPropertyException(
        String.format("Incorrect Key field '%s' type defined '%s'. '%s' type supported only by Key type '%s'",
                      keyAlias, keyType.getValue(), type, SinkKeyType.CUSTOM_NAME.getValue()),
        DatastoreSinkConstants.PROPERTY_KEY_ALIAS);
    }
  }

  private void validateIndexedProperties(Schema schema) {
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

    if (!missedProperties.isEmpty()) {
      throw new InvalidConfigPropertyException(
        String.format("Indexed properties should exist in schema. Missed properties: %s", missedProperties),
        DatastoreSinkConstants.PROPERTY_INDEXED_PROPERTIES);
    }
  }

  private void validateAncestors() {
    if (containsMacro(DatastoreSinkConstants.PROPERTY_ANCESTOR)) {
      return;
    }
    getAncestor();
  }

  private void validateBatchSize() {
    if (containsMacro(DatastoreSinkConstants.PROPERTY_BATCH_SIZE)) {
      return;
    }
    if (batchSize < 1 || batchSize > DatastoreSinkConstants.MAX_BATCH_SIZE) {
      throw new InvalidConfigPropertyException(
        String.format("Invalid Datastore batch size: '%d'. The batch size must be at least 1 and at most '%d'",
                      batchSize, DatastoreSinkConstants.MAX_BATCH_SIZE), DatastoreSinkConstants.PROPERTY_BATCH_SIZE);
    }
  }
}
