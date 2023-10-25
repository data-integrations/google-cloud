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
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.internal.bind.JsonTreeWriter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.sink.BigQueryRecordToJson;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Tests for {@link BigQueryRecordToJson}.
 */
public class BigQueryRecordToJsonTest {

  Gson gson;

  @Before
  public void setUp() {
    gson = new Gson();
  }

  @Test
  public void test() throws IOException {
    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("bytes1", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("bytes2", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("datetime", Schema.of(Schema.LogicalType.DATETIME))
    );

    byte[] bytes = "test1".getBytes();
    LocalDateTime localDateTime = LocalDateTime.now();
    StructuredRecord record = StructuredRecord.builder(schema)
      .set("int", 1)
      .set("double", 1.1d)
      .set("array", ImmutableList.of("1", "2", "3"))
      .set("bytes1", "test".getBytes())
      .set("bytes2", ByteBuffer.wrap(bytes))
      .setDateTime("datetime", localDateTime)
      .build();
    Set<String> jsonStringFieldsPaths = ImmutableSet.of("raw");

    try (JsonTreeWriter writer = new JsonTreeWriter()) {
      writer.beginObject();
      for (Schema.Field recordField : Objects.requireNonNull(record.getSchema().getFields())) {
        if (schema.getField(recordField.getName()) != null) {
          BigQueryRecordToJson.write(writer, recordField.getName(), record.get(recordField.getName()),
                                     recordField.getSchema(), jsonStringFieldsPaths);
        }
      }
      writer.endObject();

      JsonObject actual = writer.get().getAsJsonObject();
      Assert.assertEquals(1, actual.get("int").getAsInt());
      Assert.assertEquals(1.1d, actual.get("double").getAsDouble(), 0);
      Assert.assertEquals("test", new String(Base64.getDecoder().decode(actual.get("bytes1").getAsString())));
      Assert.assertEquals("test1", new String(Base64.getDecoder().decode(actual.get("bytes2").getAsString())));
      Assert.assertEquals(localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                          actual.get("datetime").getAsString());
      Iterator<JsonElement> itr = actual.get("array").getAsJsonArray().iterator();
      List<String> actualArray = new ArrayList<>();

      while (itr.hasNext()) {
        actualArray.add(itr.next().getAsString());
      }
      Assert.assertEquals(ImmutableList.of("1", "2", "3"), actualArray);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidBytes() throws IOException {
    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("bytes", Schema.of(Schema.Type.BYTES))
    );

    StructuredRecord record = StructuredRecord.builder(schema)
      .set("bytes", "test")
      .build();

    Set<String> jsonStringFieldsPaths = ImmutableSet.of("raw");
    try (JsonTreeWriter writer = new JsonTreeWriter()) {
      writer.beginObject();
      for (Schema.Field recordField : Objects.requireNonNull(record.getSchema().getFields())) {
        if (schema.getField(recordField.getName()) != null) {
          BigQueryRecordToJson.write(writer, recordField.getName(), record.get(recordField.getName()),
                                     recordField.getSchema(), jsonStringFieldsPaths);
        }
      }
      writer.endObject();
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
    Set<String> jsonStringFieldsPaths = ImmutableSet.of("raw");
    try (JsonTreeWriter writer = new JsonTreeWriter()) {
      writer.beginObject();
      for (Schema.Field recordField : Objects.requireNonNull(record.getSchema().getFields())) {
        if (schema.getField(recordField.getName()) != null) {
          BigQueryRecordToJson.write(writer, recordField.getName(), record.get(recordField.getName()),
                                     recordField.getSchema(), jsonStringFieldsPaths);
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
                                     recordField.getSchema(), null);
        }
      }
      writer.endObject();

      JsonObject actual = writer.get().getAsJsonObject();
      Assert.assertTrue(actual.get("base").getAsJsonObject().get("innerA").isJsonNull());
      Assert.assertEquals(10,
                          actual.get("base").getAsJsonObject().get("innerB").getAsJsonObject().get("int").getAsInt());
    }
  }

  @Test
  public void testNestedRecordWithJsonString() throws IOException {
    Schema nestedRecordSchema = Schema.recordOf(
            "nestedRecord",
            Schema.Field.of("nestedString", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("nestedJsonString", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("nestedInt", Schema.of(Schema.Type.INT))
    );

    Schema recordSchema = Schema.recordOf(
            "record",
            Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("jsonString", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("int", Schema.of(Schema.Type.INT)),
            Schema.Field.of("nestedRecord", nestedRecordSchema)
    );

    String jsonString = "{\"string\":\"string\",\"int\":1}";
    String nestedJsonString = "{\"nestedString\":\"nestedString\",\"nestedInt\":1}";

    StructuredRecord nestedRecord = StructuredRecord.builder(nestedRecordSchema)
            .set("nestedString", "nestedString")
            .set("nestedJsonString", nestedJsonString)
            .set("nestedInt", 1)
            .build();

    StructuredRecord record = StructuredRecord.builder(recordSchema)
            .set("string", "string")
            .set("jsonString", jsonString)
            .set("int", 1)
            .set("nestedRecord", nestedRecord)
            .build();

    JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
    JsonObject nestedJsonObject = gson.fromJson(nestedJsonString, JsonObject.class);

    Set<String> jsonStringFieldsPaths = ImmutableSet.of("jsonString", "nestedRecord.nestedJsonString");
    try (JsonTreeWriter writer = new JsonTreeWriter()) {
      writer.beginObject();
      for (Schema.Field recordField : Objects.requireNonNull(record.getSchema().getFields())) {
        if (recordSchema.getField(recordField.getName()) != null) {
          BigQueryRecordToJson.write(writer, recordField.getName(), record.get(recordField.getName()),
                  recordField.getSchema(), jsonStringFieldsPaths);
        }
      }
      writer.endObject();

      JsonObject actual = writer.get().getAsJsonObject();

      Assert.assertEquals(jsonObject, actual.get("jsonString").getAsJsonObject());
      Assert.assertEquals(nestedJsonObject, actual.get("nestedRecord").getAsJsonObject().get("nestedJsonString")
              .getAsJsonObject());
    }
  }

  @Test
  public void testJsonStringWithNestedObjects() throws IOException {
    Schema recordSchema = Schema.recordOf(
            "record",
            Schema.Field.of("jsonString", Schema.of(Schema.Type.STRING))
    );

    String jsonString = "{\n" +
            "  \"string\": \"string\",\n" +
            "  \"int\": 1,\n" +
            "  \"bool\": true,\n" +
            "  \"null\": null,\n" +
            "  \"nestedObject\": {\n" +
            "    \"nestedString\": \"nestedString\",\n" +
            "    \"nestedInt\": 1,\n" +
            "    \"nestedBool\": true,\n" +
            "    \"nestedNull\": null\n" +
            "  },\n" +
            "  \"array\": [\n" +
            "    \"string\",\n" +
            "    1,\n" +
            "    true,\n" +
            "    null,\n" +
            "    {\n" +
            "      \"nestedString\": \"nestedString\",\n" +
            "      \"nestedInt\": 1,\n" +
            "      \"nestedBool\": true,\n" +
            "      \"nestedNull\": null\n" +
            "    }\n" +
            "  ],\n" +
            "  \"nestedArray\": [\n" +
            "    [\n" +
            "      \"string\",\n" +
            "      1,\n" +
            "      true,\n" +
            "      null,\n" +
            "      {\n" +
            "        \"nestedString\": \"nestedString\",\n" +
            "        \"nestedInt\": 1,\n" +
            "        \"nestedBool\": true,\n" +
            "        \"nestedNull\": null\n" +
            "      }\n" +
            "    ]\n" +
            "  ],\n" +
            "  \"nestedObjectArray\": [\n" +
            "    {\n" +
            "      \"nestedString\": \"nestedString\",\n" +
            "      \"nestedInt\": 1,\n" +
            "      \"nestedBool\": true,\n" +
            "      \"nestedNull\": null\n" +
            "    }\n" +
            "  ]\n" +
            "}";
    JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);

    StructuredRecord record = StructuredRecord.builder(recordSchema).set("jsonString", jsonString).build();
    Set<String> jsonStringFieldsPaths = ImmutableSet.of("jsonString");

    try (JsonTreeWriter writer = new JsonTreeWriter()) {
      writer.beginObject();
      for (Schema.Field recordField : Objects.requireNonNull(record.getSchema().getFields())) {
        if (recordSchema.getField(recordField.getName()) != null) {
          BigQueryRecordToJson.write(writer, recordField.getName(), record.get(recordField.getName()),
                  recordField.getSchema(), jsonStringFieldsPaths);
        }
      }
      writer.endObject();

      JsonObject actual = writer.get().getAsJsonObject();

      Assert.assertEquals(jsonObject, actual.get("jsonString").getAsJsonObject());
    }
  }

  @Test
  public void testJsonStringWithEmptyObject() throws IOException {
    Schema recordSchema = Schema.recordOf(
            "record",
            Schema.Field.of("jsonString", Schema.of(Schema.Type.STRING))
    );
    String jsonString = "{}";
    JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
    StructuredRecord record = StructuredRecord.builder(recordSchema).set("jsonString", jsonString).build();
    Set<String> jsonStringFieldsPaths = ImmutableSet.of("jsonString");
    try (JsonTreeWriter writer = new JsonTreeWriter()) {

      writer.beginObject();
      for (Schema.Field recordField : Objects.requireNonNull(record.getSchema().getFields())) {
        if (recordSchema.getField(recordField.getName()) != null) {
          BigQueryRecordToJson.write(writer, recordField.getName(), record.get(recordField.getName()),
                  recordField.getSchema(), jsonStringFieldsPaths);
        }
      }
      writer.endObject();

      JsonObject actual = writer.get().getAsJsonObject();

      Assert.assertEquals(jsonObject, actual.get("jsonString").getAsJsonObject());
    }
  }

  @Test
  public void testJsonStringWithEmptyArray() throws IOException {
    Schema recordSchema = Schema.recordOf(
            "record",
            Schema.Field.of("jsonString", Schema.of(Schema.Type.STRING))
    );
    String jsonString = "[]";
    JsonArray jsonObject = gson.fromJson(jsonString, JsonArray.class);
    StructuredRecord record = StructuredRecord.builder(recordSchema).set("jsonString", jsonString).build();
    Set<String> jsonStringFieldsPaths = ImmutableSet.of("jsonString");
    try (JsonTreeWriter writer = new JsonTreeWriter()) {
      writer.beginObject();
      for (Schema.Field recordField : Objects.requireNonNull(record.getSchema().getFields())) {
        if (recordSchema.getField(recordField.getName()) != null) {
          BigQueryRecordToJson.write(writer, recordField.getName(), record.get(recordField.getName()),
                  recordField.getSchema(), jsonStringFieldsPaths);
        }
      }
      writer.endObject();

      JsonObject actual = writer.get().getAsJsonObject();

      Assert.assertEquals(jsonObject, actual.get("jsonString").getAsJsonArray());
    }
  }


  /**
   * Empty JSON string is not a valid JSON string and should throw an exception.
   * @throws IOException
   */
  @Test(expected = IllegalStateException.class)
  public void testEmptyJsonString() throws IOException {
    Schema recordSchema = Schema.recordOf(
            "record",
            Schema.Field.of("jsonString", Schema.of(Schema.Type.STRING))
    );
    String jsonString = "";
    StructuredRecord record = StructuredRecord.builder(recordSchema).set("jsonString", jsonString).build();
    Set<String> jsonStringFieldsPaths = ImmutableSet.of("jsonString");
    JsonTreeWriter writer = new JsonTreeWriter();
    writer.beginObject();
    for (Schema.Field recordField : Objects.requireNonNull(record.getSchema().getFields())) {
      if (recordSchema.getField(recordField.getName()) != null) {
        BigQueryRecordToJson.write(writer, recordField.getName(), record.get(recordField.getName()),
                recordField.getSchema(), jsonStringFieldsPaths);
      }
    }
    writer.endObject();
  }

}
