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
package co.cask.gcp.datastore.sink.util;

import co.cask.cdap.api.data.schema.Schema;

import java.util.List;

/**
 * Datastore schema utilities validate given CDAP schema.
 */
public class DatastoreSinkSchemaUtil {

  /**
   * Validates given CDAP schema types to be compliant with Datastore existing types.
   *
   * @param schema CDAP schema
   */
  public static void validateSchema(Schema schema) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new IllegalArgumentException("Sink schema must contain at least one field");
    }
    fields.stream()
      .map(Schema.Field::getSchema)
      .forEach(DatastoreSinkSchemaUtil::validateSinkFieldSchema);
  }

  /**
   * Validates given field schema to be complaint with Datastore types.
   * Will throw {@link IllegalArgumentException} if schema contains unsupported type.
   *
   * @param fieldSchema schema for CDAP field
   */
  private static void validateSinkFieldSchema(Schema fieldSchema) {
    switch (fieldSchema.getType()) {
      case STRING:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
      case INT:
      case FLOAT:
        return;
      case LONG:
        // timestamps in CDAP are represented as LONG with TIMESTAMP_MICROS logical type
        Schema.LogicalType logicalType = fieldSchema.getLogicalType();
        if (logicalType == null || Schema.LogicalType.TIMESTAMP_MICROS == logicalType
          || Schema.LogicalType.TIMESTAMP_MILLIS == logicalType) {
          return;
        }
        throw new IllegalArgumentException("Unsupported logical type for Long: " + logicalType);
      case RECORD:
        validateSchema(fieldSchema);
        return;
      case UNION:
        // nullable fields in CDAP are represented as UNION of NULL and FIELD_TYPE
        if (fieldSchema.isNullable()) {
          validateSinkFieldSchema(fieldSchema.getNonNullable());
          return;
        }
        throw new IllegalArgumentException("Complex UNION type is not supported");
      default:
        throw new IllegalArgumentException("Unsupported type: " + fieldSchema.getType());
    }
  }

  /**
   * Validates if key alias column is present in the schema and its type is {@link Schema.Type#STRING},
   * {@link Schema.Type#INT} or {@link Schema.Type#LONG}.
   *
   * @param schema   CDAP schema
   * @param keyType  key type
   * @param keyAlias key alias
   */
  public static void validateKeyAliasSchema(Schema schema, SinkKeyType keyType, String keyAlias) {
    Schema.Field field = schema.getField(keyAlias);
    if (field == null) {
      throw new IllegalArgumentException(String.format("Key Alias [%s] should exist in schema.", keyAlias));
    }

    Schema.Type type = field.getSchema().getType();
    if (Schema.Type.STRING != type && Schema.Type.LONG != type && Schema.Type.INT != type) {
      String foundType = field.getSchema().isNullable()
        ? "nullable " + field.getSchema().getNonNullable().getType().name()
        : type.name();
      throw new IllegalArgumentException(
        String.format("Key field [%s] type must be non-nullable STRING, INT or LONG, not [%s]", keyAlias,
                                                       foundType));
    } else if ((Schema.Type.LONG == type || Schema.Type.INT == type) && keyType != SinkKeyType.CUSTOM_NAME) {
      throw new IllegalArgumentException(
        String.format("Incorrect Key field [%s] type defined [%s]. [%s] type supported only by Key type [%s]",
                      keyAlias, keyType.getValue(), type, SinkKeyType.CUSTOM_NAME.getValue()));
    }
  }
}
