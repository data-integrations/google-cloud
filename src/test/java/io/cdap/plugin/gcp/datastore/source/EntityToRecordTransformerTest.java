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
package io.cdap.plugin.gcp.datastore.source;

import com.google.cloud.datastore.Blob;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import com.google.type.LatLng;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.datastore.source.util.SourceKeyType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Arrays;

/**
 * Tests for {@link EntityToRecordTransformer} class.
 */
public class EntityToRecordTransformerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testTransformAllTypes() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("timestamp_field", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("blob_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("entity_field", Schema.nullableOf(Schema.recordOf("entity_field",
          Schema.Field.of("nested_string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("nested_long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))))),
      Schema.Field.of("list_field",
                      Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING))))));

    Instant time = Instant.now();
    Timestamp entityTs = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
      .setNanos(time.getNano()).build();
    Entity nestedEntity = Entity.newBuilder()
      .putProperties("nested_string_field", Value.newBuilder().setStringValue("nested_value")
        .build())
      .putProperties("nested_long_field", Value.newBuilder().setIntegerValue(20L).build())
      .build();
    ArrayValue arrayValue = ArrayValue.newBuilder().addAllValues(Arrays.asList(
      Value.newBuilder().setStringValue("value_1").build(),
      Value.newBuilder().setStringValue("value_2").build())).build();

    Entity entity = Entity.newBuilder()
      .setKey(Key.newBuilder().
        setPartitionId(PartitionId.newBuilder()
                         .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT)))
      .putProperties("string_field", Value.newBuilder().setStringValue("string_value").build())
      .putProperties("long_field", Value.newBuilder().setIntegerValue(10L).build())
      .putProperties("double_field", Value.newBuilder().setDoubleValue(10.5D).build())
      .putProperties("boolean_field", Value.newBuilder().setBooleanValue(true).build())
      .putProperties("timestamp_field", Value.newBuilder().setTimestampValue(entityTs).build())
      .putProperties("blob_field", Value.newBuilder().
        setBlobValue(ByteString.copyFrom(Blob.copyFrom("test_blob".getBytes()).toByteArray())).build())
      .putProperties("null_field", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
      .putProperties("entity_field", Value.newBuilder().setEntityValue(nestedEntity).build())
      .putProperties("list_field", Value.newBuilder().setArrayValue(arrayValue).build())
      .putProperties("lat_lng_field", Value.newBuilder().setGeoPointValue(
        LatLng.newBuilder()
          .setLatitude(10)
          .setLongitude(5)
          .build()).build())
      .putProperties("key_field", Value.newBuilder().setKeyValue(
        Key.newBuilder()
          .setPartitionId(PartitionId.newBuilder()
                            .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
          .addPath(Key.PathElement.newBuilder()
                     .setId(2)
                     .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
          .build()
      ).build())
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.NONE, "key");
    StructuredRecord record = transformer.transformEntity(entity);

    Assert.assertEquals("string_value", record.get("string_field"));
    Assert.assertEquals(10L, (long) record.get("long_field"));
    Assert.assertEquals(10.5D, record.get("double_field"), 0);

    ZonedDateTime recordTs = record.getTimestamp("timestamp_field");
    Timestamp actualTs = Timestamp.newBuilder()
      .setSeconds(time.getEpochSecond())
      .setNanos(time.getNano()).build();
    Assert.assertEquals(entityTs, actualTs);

    Assert.assertTrue(record.get("boolean_field"));
    Assert.assertEquals("test_blob", new String((byte[]) record.get("blob_field")));
    Assert.assertNull(record.get("null_field"));

    StructuredRecord entityRecord = record.get("entity_field");
    Assert.assertEquals("nested_value", entityRecord.get("nested_string_field"));
    Assert.assertEquals(20L, (long) entityRecord.get("nested_long_field"));

    Assert.assertEquals(Arrays.asList("value_1", "value_2"), record.get("list_field"));
  }

  @Test
  public void testTransformWithKeyLiteral() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("key", Schema.of(Schema.Type.STRING)));

    Entity entity = Entity.newBuilder()
      .setKey(Key.newBuilder()
                .setPartitionId(PartitionId.newBuilder()
                                  .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
                .addPath(Key.PathElement.newBuilder()
                           .setId(1)
                           .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
                .build())
                .putProperties("string_field", Value.newBuilder().setStringValue("string_value").build())
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.KEY_LITERAL, "key");
    StructuredRecord record = transformer.transformEntity(entity);

    Assert.assertEquals("string_value", record.get("string_field"));
    Assert.assertEquals(String.format("key(%s, %s)", DatastoreSourceConfigHelper.TEST_KIND, 1), record.get("key"));
  }

  @Test
  public void testTransformWithUrlSafeKey() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("key", Schema.of(Schema.Type.STRING)));

    Key key = Key.newBuilder()
      .addPath(Key.PathElement.newBuilder()
                 .setId(1)
                 .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
      .build();

    Entity entity = Entity.newBuilder()
      .setKey(key)
      .putProperties("string_field", Value.newBuilder().setStringValue("string_value").build())
      .build();


    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.URL_SAFE_KEY, "key");
    StructuredRecord record = transformer.transformEntity(entity);

    Assert.assertEquals("string_value", record.get("string_field"));
    String encodedKey = "";
    try {
      encodedKey = URLEncoder.encode(key.toString(), StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      Assert.fail(String.format("Failed to encode key: %s", e));
    }
    Assert.assertEquals(encodedKey, record.get("key"));
  }

  @Test
  public void testTransformWithoutKey() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)));

    Entity entity = Entity.newBuilder()
      .setKey(Key.newBuilder()
                .setPartitionId(PartitionId.newBuilder()
                                  .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
                .addPath(Key.PathElement.newBuilder()
                           .setId(1)
                           .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
                .build())
      .putProperties("string_field", Value.newBuilder().setStringValue("string_value").build())
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.NONE, "key");
    StructuredRecord record = transformer.transformEntity(entity);

    Assert.assertEquals("string_value", record.get("string_field"));
  }

  @Test
  public void testTransformNullIntoNotNull() {
    String fieldName = "string_field";
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of(fieldName, Schema.of(Schema.Type.STRING)));

    Entity entity = Entity.newBuilder()
      .setKey(Key.newBuilder()
                .setPartitionId(PartitionId.newBuilder()
                                  .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
                .addPath(Key.PathElement.newBuilder()
                           .setId(1)
                           .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
                .build())
      .putProperties(fieldName, Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
      .build();

    thrown.expect(UnexpectedFormatException.class);
    thrown.expectMessage(fieldName);

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.NONE, "key");
    transformer.transformEntity(entity);
  }

  @Test
  public void testTransformMissingField() {
    Schema schema = Schema.recordOf("schema",
       Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
       Schema.Field.of("missing_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    Entity entity = Entity.newBuilder()
      .setKey(Key.newBuilder()
                .setPartitionId(PartitionId.newBuilder()
                                  .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
                .addPath(Key.PathElement.newBuilder()
                           .setId(1)
                           .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
                .build())
      .putProperties("string_field", Value.newBuilder().setStringValue("string_value").build())
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.NONE, "key");
    StructuredRecord record = transformer.transformEntity(entity);

    Assert.assertEquals("string_value", record.get("string_field"));
    Assert.assertNull(record.get("missing_field"));
  }

  @Test
  public void testTransformDifferentType() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("field", Schema.of(Schema.Type.LONG)));

    Entity entity = Entity.newBuilder()
      .setKey(Key.newBuilder()
                .setPartitionId(PartitionId.newBuilder()
                                  .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
                .addPath(Key.PathElement.newBuilder()
                           .setId(1)
                           .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
                .build())
      .putProperties("field", Value.newBuilder().setStringValue("field_value").build())
      .build();

    thrown.expect(UnexpectedFormatException.class);

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.NONE, "key");
    transformer.transformEntity(entity);
  }

  @Test
  public void testTransformOnlyKey() {
    String keyField = "key";
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of(keyField, Schema.of(Schema.Type.STRING)));

    Entity entity = Entity.newBuilder()
      .setKey(Key.newBuilder()
                .setPartitionId(PartitionId.newBuilder()
                                  .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
                .addPath(Key.PathElement.newBuilder()
                           .setId(1)
                           .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
                .build())
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.KEY_LITERAL, keyField);
    StructuredRecord record = transformer.transformEntity(entity);

    Assert.assertEquals(String.format("key(%s, %s)", DatastoreSourceConfigHelper.TEST_KIND, 1), record.get(keyField));
  }

  @Test
  public void testTransformKeyToKeyStringKeyLiteral() {
    Key key = Key.newBuilder()
      .setPartitionId(PartitionId.newBuilder()
                        .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
      .addPath(Key.PathElement.newBuilder()
                 .setId(10)
                 .setKind("A1")
        .build())
      .addPath(Key.PathElement.newBuilder()
                 .setName("N1")
                 .setKind("A2")
                 .build())
      .addPath(Key.PathElement.newBuilder()
                 .setId(1)
                 .setKind("key")
                 .build())
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(null, SourceKeyType.KEY_LITERAL, "key");

    Assert.assertEquals("key(A1, 10, A2, 'N1', key, 1)", transformer.transformKeyToKeyString(key));
  }

  @Test
  public void testTransformKeyToKeyStringUrlSafeKey() {
    Key key = Key.newBuilder()
      .setPartitionId(PartitionId.newBuilder()
                        .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT)
                        .setNamespaceId(DatastoreSourceConfigHelper.TEST_NAMESPACE)
                        .build())
      .addPath(Key.PathElement.newBuilder()
                 .setId(10)
                 .setKind("A1")
                 .build())
      .addPath(Key.PathElement.newBuilder()
                 .setName("N1")
                 .setKind("A2")
                 .build())
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(null, SourceKeyType.URL_SAFE_KEY, "key");
    String encodedKey = "";
    try {
      encodedKey = URLEncoder.encode(key.toString(), StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      Assert.fail(String.format("Failed to encode key: %s", e));
    }
    Assert.assertEquals(encodedKey, transformer.transformKeyToKeyString(key));
  }

  @Test
  public void testTransformComplexUnion() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("union_field_string",
        Schema.unionOf(Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING))),
      Schema.Field.of("union_field_long",
        Schema.unionOf(Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING))),
      Schema.Field.of("union_field_null",
        Schema.unionOf(Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL))));
    Entity entity = Entity.newBuilder()
      .setKey(Key.newBuilder()
                .setPartitionId(PartitionId.newBuilder()
                                  .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
                .addPath(Key.PathElement.newBuilder()
                           .setId(1)
                           .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
                .build())
      .putProperties("union_field_string", Value.newBuilder().setStringValue("string_value").build())
      .putProperties("union_field_long", Value.newBuilder().setIntegerValue(10).build())
      .putProperties("union_field_null", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.NONE, "key");
    StructuredRecord record = transformer.transformEntity(entity);
    Assert.assertEquals("string_value", record.get("union_field_string"));
    Assert.assertEquals(10, (long) record.get("union_field_long"));
    Assert.assertNull(record.get("union_field_null"));
  }

  @Test
  public void testTransformComplexUnionUndeclaredSchema() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("union_field", Schema.unionOf(Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING))));

    Entity entity = Entity.newBuilder()
      .setKey(Key.newBuilder()
                .setPartitionId(PartitionId.newBuilder()
                                  .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
                .addPath(Key.PathElement.newBuilder()
                           .setId(1)
                           .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
                .build())
      .putProperties("union_field", Value.newBuilder().setBooleanValue(true).build())
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.NONE, "key");

    thrown.expect(IllegalStateException.class);

    transformer.transformEntity(entity);
  }

  @Test
  public void testTransformArrayWithComplexUnion() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("array_field", Schema.nullableOf(Schema.arrayOf(
        Schema.unionOf(Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING))))));

    ArrayValue arrayValue = ArrayValue.newBuilder().addAllValues(Arrays.asList(
      Value.newBuilder().setStringValue("string_value").build(),
      Value.newBuilder().setIntegerValue(10).build(),
      Value.newBuilder().setIntegerValue(20).build()))
      .build();
    Entity entity = Entity.newBuilder()
      .setKey(Key.newBuilder().
        setPartitionId(PartitionId.newBuilder()
                         .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
                .addPath(Key.PathElement.newBuilder()
                 .setId(1)
                 .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
                .build()
      )
      .putProperties("array_field", Value.newBuilder().setArrayValue(arrayValue).build())
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.NONE, "key");
    StructuredRecord record = transformer.transformEntity(entity);
    Object fieldValue = record.get("array_field");
    Assert.assertNotNull(fieldValue);
    Assert.assertEquals(Arrays.asList("string_value", 10, 20).toString(), fieldValue.toString());
  }

}
