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
package io.cdap.plugin.gcp.bigquery.sink;

import com.google.gson.JsonObject;
import com.google.gson.internal.bind.JsonTreeWriter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.RecordConverter;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * BigQueryJsonConverter converts a {@link StructuredRecord} to {@link JsonObject}
 */
public class BigQueryJsonConverter extends RecordConverter<StructuredRecord, JsonObject> {
  private Set<String> jsonStringFieldsPaths;

  public BigQueryJsonConverter() {
  }

  public BigQueryJsonConverter(Set<String> jsonStringFieldsPaths) {
    this.jsonStringFieldsPaths = jsonStringFieldsPaths;
  }

  @Override
  public JsonObject transform(StructuredRecord input, @Nullable Schema schema) throws IOException {
    try (JsonTreeWriter writer = new JsonTreeWriter()) {
      writer.beginObject();
      for (Schema.Field recordField : Objects.requireNonNull(input.getSchema().getFields())) {
        // From all the fields in input record, write only those fields that are present in output schema
        if (schema != null && schema.getField(recordField.getName()) == null) {
          continue;
        }
        BigQueryRecordToJson.write(writer, recordField.getName(), input.get(recordField.getName()),
                                   recordField.getSchema(), jsonStringFieldsPaths);
      }
      writer.endObject();
      return writer.get().getAsJsonObject();
    }
  }
}
