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

  @Test
  public void testArrayOfRecords() throws IOException {
    Schema innerSchema = Schema.recordOf(
      "address",
      Schema.Field.of("city", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("street", Schema.of(Schema.Type.STRING))
    );
    Schema recordSchema = Schema.recordOf(
      "addresses",
      Schema.Field.of("address", innerSchema)
    );

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("addresses", Schema.arrayOf(recordSchema)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("addresses", ImmutableList.of(StructuredRecord.builder(recordSchema)
                                           .set("address", StructuredRecord.builder(innerSchema)
                                             .set("city", "city1")
                                             .set("street", "street1")
                                             .build())
                                           .build(),
                                         StructuredRecord.builder(recordSchema)
                                           .set("address", StructuredRecord.builder(innerSchema)
                                             .set("city", "city2")
                                             .set("street", "street2")
                                             .build())
                                           .build()))
      .set("name", "Tod")
      .build();

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
      Iterator<JsonElement> itr = actual.get("addresses").getAsJsonArray().iterator();

      List<JsonElement> actualArray = new ArrayList<>();
      while (itr.hasNext()) {
        actualArray.add(itr.next());
      }

      Assert.assertEquals(2, actualArray.size());

      int counter = 1;
      for (JsonElement element : actualArray) {
        Assert.assertEquals(("city" + counter),
                            element.getAsJsonObject().get("address").getAsJsonObject().get("city").getAsString());
        Assert.assertEquals(("street" + counter),
                            element.getAsJsonObject().get("address").getAsJsonObject().get("street").getAsString());
        counter++;
      }
    }
  }

  @Test
  public void testNullableRecord() throws IOException {
    Schema innerSchemaA = Schema.unionOf(Schema.recordOf(
      "innerA",
      Schema.Field.of("sting", Schema.of(Schema.Type.STRING))
    ), Schema.of(Schema.Type.NULL));
    Schema innerSchemaB = Schema.recordOf(
      "innerB",
      Schema.Field.of("int", Schema.of(Schema.Type.INT))
    );
    Schema baseSchema = Schema.recordOf(
      "base",
      Schema.Field.of("innerA", innerSchemaA),
      Schema.Field.of("innerB", innerSchemaB)
    );

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("base", baseSchema)
    );

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("base", StructuredRecord.builder(baseSchema)
        .set("innerA", null)
        .set("innerB", StructuredRecord.builder(innerSchemaB)
          .set("int", 10)
          .build())
        .build())
      .build();

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
      Assert.assertTrue(actual.get("base").getAsJsonObject().get("innerA").isJsonNull());
      Assert.assertEquals(10,
                          actual.get("base").getAsJsonObject().get("innerB").getAsJsonObject().get("int").getAsInt());
    }
  }
}
