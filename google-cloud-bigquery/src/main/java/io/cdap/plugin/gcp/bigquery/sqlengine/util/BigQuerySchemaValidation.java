/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.sqlengine.util;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Class used to validate if a CDAP schema is supported in BigQuery for Pushdown operations.
 */
public final class BigQuerySchemaValidation {
  private final boolean isSupported;
  private final List<String> invalidFields;

  public BigQuerySchemaValidation(boolean isSupported, List<String> invalidFields) {
    this.isSupported = isSupported;
    this.invalidFields = invalidFields;
  }

  public boolean isSupported() {
    return isSupported;
  }

  public List<String> getInvalidFields() {
    return invalidFields;
  }

  /**
   * Validate that this schema is supported in BigQuery.
   * <p>
   * We don't support CDAP types ENUM, MAP and UNION in BigQuery. However, we do support Unions
   *
   * @param schema input schema
   * @return boolean determining if this schema is supported in BQ.
   */
  @VisibleForTesting
  protected static BigQuerySchemaValidation validateSchema(Schema schema) {
    List<String> invalidFields = new ArrayList<>();

    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.getSchema();

      // For nullable types, check the underlying type.
      if (fieldSchema.isNullable()) {
        fieldSchema = fieldSchema.getNonNullable();
      }

      if (fieldSchema.getType() == Schema.Type.ENUM || fieldSchema.getType() == Schema.Type.MAP
        || fieldSchema.getType() == Schema.Type.UNION) {
        invalidFields.add(field.getName());
      }
    }

    return new BigQuerySchemaValidation(invalidFields.isEmpty(), invalidFields);
  }
}
