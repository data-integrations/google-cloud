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

package io.cdap.plugin.gcp.bigquery;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.internal.bind.JsonTreeWriter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.sink.BigQueryRecordToJson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Tests for {@link BigQueryRecordToJson}.
 */
public class BigQueryRecordToJsonTest {

  @Test
  public void test() throws IOException {
    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.STRING)))
    );

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("int", 1)
      .set("double", 1.1d)
      .set("array", ImmutableList.of("1", "2", "3")).build();

    try (JsonTreeWriter writer = new JsonTreeWriter()) {
      writer.beginObject();
      for (Schema.Field recordField : Objects.requireNonNull(record.getSchema().getFields())) {
        if (schema.getField(recordField.getName()) != null) {
          BigQueryRecordToJson.write(writer, recordField.getName(), record.get(recordField.getName()),
                                     recordField.getSchema());
        }
      }
      writer.endObject();

      JsonObject actual = writer.get().getAsJsonObject();
      Assert.assertEquals(1, actual.get("int").getAsInt());
      Assert.assertEquals(1.1d, actual.get("double").getAsDouble(), 0);
      Iterator<JsonElement> itr = actual.get("array").getAsJsonArray().iterator();
      List<String> actualArray = new ArrayList<>();

      while (itr.hasNext()) {
        actualArray.add(itr.next().getAsString());
      }
      Assert.assertEquals(ImmutableList.of("1", "2", "3"), actualArray);
    }
  }
}
