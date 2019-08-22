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

package io.cdap.plugin.gcp.common;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

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
   * @param failureCollector the failure collector to collect validation failures
   */
  public static void validateFieldsMatch(Schema trueSchema, Schema providedSchema, FailureCollector failureCollector) {
    for (Schema.Field field : providedSchema.getFields()) {
      Schema.Field trueField = trueSchema.getField(field.getName());
      if (trueField == null) {
        failureCollector.addFailure(String.format("Field '%s' does not exist in the schema.", field.getName()), null)
          .withOutputSchemaField(field.getName());
        return;
      }
      Schema trueFieldSchema = trueField.getSchema();
      Schema providedFieldSchema = field.getSchema();

      boolean isTrueFieldNullable = trueFieldSchema.isNullable();
      boolean isProvidedFieldNullable = providedFieldSchema.isNullable();

      Schema trueNonNullable = isTrueFieldNullable ? trueFieldSchema.getNonNullable() : trueFieldSchema;
      Schema providedNonNullable = isProvidedFieldNullable ? providedFieldSchema.getNonNullable() : providedFieldSchema;

      if (trueNonNullable.getLogicalType() != providedNonNullable.getLogicalType() ||
        trueNonNullable.getType() != providedNonNullable.getType()) {
        failureCollector.addFailure(String.format("Field '%s' is of unexpected type '%s'.",
                                                  field.getName(), providedNonNullable.getDisplayName()),
                                    String.format("It must be of type '%s'.", trueNonNullable.getDisplayName()))
          .withOutputSchemaField(field.getName());
      }

      if (!isTrueFieldNullable && isProvidedFieldNullable) {
        failureCollector.addFailure(String.format("Field '%s' must not be nullable.", field.getName()), null)
          .withOutputSchemaField(field.getName());
      }
    }
  }
}
