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

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.LatLng;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.StringValue;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.datastore.source.util.SourceKeyType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

    Timestamp entityTs = Timestamp.now();
    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("string_field", "string_value")
      .set("long_field", 10L)
      .set("double_field", 10.5D)
      .set("boolean_field", true)
      .set("timestamp_field", entityTs)
      .set("blob_field", Blob.copyFrom("test_blob".getBytes()))
      .setNull("null_field")
      .set("entity_field", Entity.newBuilder()
        .set("nested_string_field", "nested_value")
        .set("nested_long_field", 20L)
        .build())
      .set("list_field", "value_1", "value_2")
      .set("key_field", Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                       DatastoreSourceConfigHelper.TEST_KIND, 2).build())
      .set("lat_lng_field", LatLng.of(10, 5))
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.NONE, "key");
    StructuredRecord record = transformer.transformEntity(entity);

    Assert.assertEquals("string_value", record.get("string_field"));
    Assert.assertEquals(10L, (long) record.get("long_field"));
    Assert.assertEquals(10.5D, record.get("double_field"), 0);

    ZonedDateTime recordTs = record.getTimestamp("timestamp_field");
    Timestamp actualTs = Timestamp.ofTimeSecondsAndNanos(recordTs.toEpochSecond(), recordTs.getNano());
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

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("string_field", "string_value")
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

    Key key = Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT, DatastoreSourceConfigHelper.TEST_KIND, 1)
      .build();
    Entity entity = Entity.newBuilder(key)
      .set("string_field", "string_value")
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.URL_SAFE_KEY, "key");
    StructuredRecord record = transformer.transformEntity(entity);

    Assert.assertEquals("string_value", record.get("string_field"));
    Assert.assertEquals(key.toUrlSafe(), record.get("key"));
  }

  @Test
  public void testTransformWithoutKey() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)));

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("string_field", "string_value")
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

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .setNull(fieldName)
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

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("string_field", "string_value")
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

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("field", "field_value")
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

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.KEY_LITERAL, keyField);
    StructuredRecord record = transformer.transformEntity(entity);

    Assert.assertEquals(String.format("key(%s, %s)", DatastoreSourceConfigHelper.TEST_KIND, 1), record.get(keyField));
  }

  @Test
  public void testTransformKeyToKeyStringKeyLiteral() {
    Key key = Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT, "key", 1)
      .setNamespace(DatastoreSourceConfigHelper.TEST_NAMESPACE)
      .addAncestor(PathElement.of("A1", 10))
      .addAncestor(PathElement.of("A2", "N1"))
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(null, SourceKeyType.KEY_LITERAL, "key");

    Assert.assertEquals("key(A1, 10, A2, 'N1', key, 1)", transformer.transformKeyToKeyString(key));
  }

  @Test
  public void testTransformKeyToKeyStringUrlSafeKey() {
    Key key = Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT, "key", 1)
      .setNamespace(DatastoreSourceConfigHelper.TEST_NAMESPACE)
      .addAncestor(PathElement.of("A1", 10))
      .addAncestor(PathElement.of("A2", "N1"))
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(null, SourceKeyType.URL_SAFE_KEY, "key");
    Assert.assertEquals(key.toUrlSafe(), transformer.transformKeyToKeyString(key));
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

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("union_field_string", "string_value")
      .set("union_field_long", 10)
      .setNull("union_field_null")
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

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("union_field", true)
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

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("array_field", StringValue.of("string_value"), LongValue.of(10), LongValue.of(20))
      .build();

    EntityToRecordTransformer transformer = new EntityToRecordTransformer(schema, SourceKeyType.NONE, "key");
    StructuredRecord record = transformer.transformEntity(entity);
    Object fieldValue = record.get("array_field");
    Assert.assertNotNull(fieldValue);
    Assert.assertEquals(Arrays.asList("string_value", 10, 20).toString(), fieldValue.toString());
  }

}
