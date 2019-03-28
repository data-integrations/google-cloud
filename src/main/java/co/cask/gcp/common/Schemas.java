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

package co.cask.gcp.common;

import co.cask.cdap.api.data.schema.Schema;

/**
 * Utility class for schemas.
 */
public class Schemas {

  private Schemas() {
    // no-op
  }

  /**
   * Validate that the provided schema is compatible with the true schema. The provided schema is compatible if every
   * field is compatible with the corresponding field in the true schema. A field is compatible if it is of the same
   * type or is a nullable version of that type. It is assumed that both schemas are record schemas.
   * 
   * @param trueSchema the true schema
   * @param providedSchema the provided schema to check compatibility
   * @throws IllegalArgumentException if the schemas are not type compatible
   */
  public static void validateFieldsMatch(Schema trueSchema, Schema providedSchema) {
    for (Schema.Field field : providedSchema.getFields()) {
      Schema.Field trueField = trueSchema.getField(field.getName());
      if (trueField == null) {
        throw new IllegalArgumentException(String.format("Field '%s' does not exist", field.getName()));
      }
      Schema trueFieldSchema = trueField.getSchema();
      Schema providedFieldSchema = field.getSchema();

      boolean isTrueFieldNullable = trueFieldSchema.isNullable();
      boolean isProvidedFieldNullable = providedFieldSchema.isNullable();

      Schema.Type trueFieldType = isTrueFieldNullable ?
        trueFieldSchema.getNonNullable().getType() : trueFieldSchema.getType();
      Schema.Type providedFieldType = isProvidedFieldNullable ?
        providedFieldSchema.getNonNullable().getType() : providedFieldSchema.getType();

      if (trueFieldType != providedFieldType) {
        throw new IllegalArgumentException(
          String.format("Expected field '%s' to be of type '%s', but it is of type '%s'.",
                        field.getName(), providedFieldType.name(), trueFieldType.name()));
      }
      if (isTrueFieldNullable && !isProvidedFieldNullable) {
        throw new IllegalArgumentException(String.format("Field '%s' should not be nullable.", field.getName()));
      }
    }
  }
}
