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
package co.cask.gcp.datastore.source.util;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.datastore.source.DatastoreSourceConfig;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Value;
import com.google.cloud.datastore.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Datastore schema utilities help to convert Datastore schema into CDAP schema,
 * and validate given CDAP schema.
 */
public class DatastoreSourceSchemaUtil {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreSourceSchemaUtil.class);

  /**
   * Validates if key alias column is present in the schema and its type is {@link Schema.Type#STRING}.
   *
   * @param schema CDAP schema
   * @param keyAlias key alias
   */
  public static void validateKeyAlias(Schema schema, String keyAlias) {
    Schema.Field field = schema.getField(keyAlias);
    if (field == null) {
      throw new IllegalArgumentException(String.format("Key field [%s] must exist in the schema.", keyAlias));
    }

    Schema fieldSchema = field.getSchema();
    Schema.Type type = fieldSchema.getType();
    if (Schema.Type.STRING != type) {
      String foundType = fieldSchema.isNullable()
        ? "nullable " + fieldSchema.getNonNullable().getType().name()
        : type.name();
      throw new IllegalArgumentException(String.format("Key field [%s] type must be non-nullable STRING, not %s",
          keyAlias, foundType));
    }
  }

  /**
   * Validates given CDAP schema types to be compliant with Datastore existing types.
   *
   * @param schema CDAP schema
   */
  public static void validateSchema(Schema schema) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new IllegalArgumentException("Source schema must contain at least one field");
    }
    fields.stream()
      .map(Schema.Field::getSchema)
      .forEach(DatastoreSourceSchemaUtil::validateFieldSchema);
  }

  /**
   * Constructs CDAP schema based on given CDAP entity and source configuration,
   * will add Datastore key to the list of schema fields if config include key flag is set to true.
   *
   * @param entity Datastore entity
   * @param config Datastore source config
   * @return CDAP schema
   */
  public static Schema constructSchema(Entity entity, DatastoreSourceConfig config) {
    List<Schema.Field> fields = constructSchemaFields(entity);

    if (config.isIncludeKey()) {
      fields.add(Schema.Field.of(config.getKeyAlias(), Schema.of(Schema.Type.STRING)));
    }

    return Schema.recordOf("schema", fields);
  }

  /**
   * Validates given field schema to be complaint with Datastore types.
   * Will throw {@link IllegalArgumentException} if schema contains unsupported type.
   *
   * @param fieldSchema schema for CDAP field
   */
  private static void validateFieldSchema(Schema fieldSchema) {
    switch (fieldSchema.getType()) {
      case STRING:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
        return;
      case LONG:
        // timestamps in CDAP are represented as LONG with TIMESTAMP_MICROS logical type
        Schema.LogicalType logicalType = fieldSchema.getLogicalType();
        if (logicalType == null || Schema.LogicalType.TIMESTAMP_MICROS == logicalType) {
          return;
        }
        throw new IllegalArgumentException("Unsupported logical type for Long: " + logicalType);
      case RECORD:
        validateSchema(fieldSchema);
        return;
      case UNION:
        // nullable fields in CDAP are represented as UNION of NULL and FIELD_TYPE
        if (fieldSchema.isNullable()) {
          validateFieldSchema(fieldSchema.getNonNullable());
          return;
        }
        throw new IllegalArgumentException("Complex UNION type is not supported");
      default:
        throw new IllegalArgumentException("Unsupported type: " + fieldSchema.getType());
    }
  }

  /**
   * Constructs list of CDAP schema fields based on given Datastore entity,
   * filters out fields schemas with null value.
   *
   * @param entity Datastore entity
   * @return list of CDAP schema fields
   */
  private static List<Schema.Field> constructSchemaFields(FullEntity<?> entity) {
    return entity.getNames().stream()
      .map(name -> transformToField(name, entity.getValue(name)))
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  /**
   * Since Datastore is schemaless database, returns nullable field schema for the given value type.
   * For {@link ValueType#NULL} will return default {@link Schema.Type#STRING},
   * for unsupported types will return null.
   *
   * @param name field name
   * @param value Datastore value type
   * @return CDAP field
   */
  private static Schema.Field transformToField(String name, Value<?> value) {
    switch (value.getType()) {
      case STRING:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      case LONG:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.LONG)));
      case DOUBLE:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)));
      case BOOLEAN:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)));
      case TIMESTAMP:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));
      case BLOB:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.BYTES)));
      case NULL:
        // Datastore stores null values in Null Type unlike regular databases which have nullable type definition
        // set default String type, instead of skipping the field
        LOG.debug("Unable to determine type, setting default type {} for the field {}", Schema.Type.STRING, name);
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      case ENTITY:
        List<Schema.Field> fields = constructSchemaFields(((EntityValue) value).get());
        return Schema.Field.of(name, Schema.nullableOf(Schema.recordOf(name, fields)));
      case LIST:
      case KEY:
      case LAT_LNG:
      case RAW_VALUE:
      default:
        LOG.debug("Unsupported type {} for field, skipping field from the schema", value.getType(), name);
        return null;
    }
  }
}
