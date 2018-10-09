/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.gcp.bigquery;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.time.format.DateTimeFormatter;
import javax.annotation.Nullable;

/**
 * Converts StructruredRecord into JsonObject.
 */
public class StructuredRecordToJsonTransformer {
  private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  private final String skipField;

  public StructuredRecordToJsonTransformer(@Nullable String skipField) {
    this.skipField = skipField;
  }

  public JsonObject transform(StructuredRecord record) {
    JsonObject object = new JsonObject();
    for (Schema.Field field : record.getSchema().getFields()) {
      if (field.getName().equals(skipField)) {
        continue;
      }
      decodeSimpleTypes(object, field.getName(), record);
    }
    return object;
  }

  private static void decodeSimpleTypes(JsonObject json, String name, StructuredRecord input) {
    Object object = input.get(name);
    Schema schema = BigQueryUtils.getNonNullableSchema(input.getSchema().getField(name).getSchema());

    if (object == null) {
      json.add(name, JsonNull.INSTANCE);
      return;
    }

    Schema.LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          json.addProperty(name, input.getDate(name).toString());
          break;
        case TIME_MILLIS:
        case TIME_MICROS:
          json.addProperty(name, input.getTime(name).toString());
          break;
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          //timestamp for json input should be in this format yyyy-MM-dd HH:mm:ss.SSSSSS
          json.addProperty(name, dtf.format(input.getTimestamp(name)));
          break;
        default:
          throw new IllegalStateException(String.format("Unsupported logical type %s", logicalType));
      }
      return;
    }

    Schema.Type type = schema.getType();
    switch (type) {
      case NULL:
        json.add(name, JsonNull.INSTANCE); // nothing much to do here.
        break;
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        json.addProperty(name, (Number) object);
        break;
      case BOOLEAN:
        json.addProperty(name, (Boolean) object);
        break;
      case STRING:
        json.addProperty(name, object.toString());
        break;
      default:
        throw new IllegalStateException(String.format("Unsupported type %s", type));
    }
  }
}
