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
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.StringValue;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

/**
 * Tests for {@link DatastoreSource} class.
 */
@RunWith(MockitoJUnitRunner.class)
public class DatastoreSourceTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @InjectMocks
  private DatastoreSource datastoreSource;

  @Test
  public void testGetSchemaIsIncludeKeyTrue() {
    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("string_field", "string_value")
      .build();

    Schema schemaWithKey = datastoreSource.constructSchema(entity, true, "key");

    List<Schema.Field> schemaWithKeyFields = schemaWithKey.getFields();
    Assert.assertNotNull(schemaWithKeyFields);
    Assert.assertEquals(2, schemaWithKeyFields.size());
    checkField("string_field", schemaWithKey, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    checkField("key", schemaWithKey, Schema.of(Schema.Type.STRING));
  }

  @Test
  public void testGetSchemaIsIncludeKeyFalse() {
    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("string_field", "string_value")
      .set("long_field", 10L)
      .set("double_field", 10.5D)
      .set("boolean_field", true)
      .set("timestamp_field", Timestamp.now())
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

    Schema schema = datastoreSource.constructSchema(entity, false, "key");

    List<Schema.Field> fields = schema.getFields();
    Assert.assertNotNull(fields);
    Assert.assertEquals(9, fields.size());
    checkField("string_field", schema, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    checkField("long_field", schema, Schema.nullableOf(Schema.of(Schema.Type.LONG)));
    checkField("double_field", schema, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)));
    checkField("boolean_field", schema, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)));
    checkField("timestamp_field", schema, Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));
    checkField("blob_field", schema, Schema.nullableOf(Schema.of(Schema.Type.BYTES)));
    checkField("null_field", schema, Schema.of(Schema.Type.NULL));
    checkField("list_field", schema,
               Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))));

    Schema.Field entityField = schema.getField("entity_field");
    Assert.assertNotNull(entityField);
    Assert.assertTrue(entityField.getSchema().isNullable());
    Schema entitySchema = entityField.getSchema().getNonNullable();
    Assert.assertEquals(Schema.Type.RECORD, schema.getType());
    List<Schema.Field> entityFields = entitySchema.getFields();
    Assert.assertNotNull(entityFields);
    Assert.assertEquals(2, entityFields.size());
    checkField("nested_string_field", entitySchema, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    checkField("nested_long_field", entitySchema, Schema.nullableOf(Schema.of(Schema.Type.LONG)));
  }

  @Test
  public void testGetSchemaArrayWithComplexUnion() {
    LongValue longValue1 = LongValue.of(10);
    LongValue longValue2 = LongValue.of(20);
    StringValue stringValue1 = StringValue.of("string_value_1");
    StringValue stringValue2 = StringValue.of("string_value_2");

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("array_field", longValue1, longValue2, stringValue1, stringValue2)
      .set("array_field_null", NullValue.of(), NullValue.of())
      .set("array_field_empty", ListValue.newBuilder().build())
      .build();

    Schema schema = datastoreSource.constructSchema(entity, false, "key");
    checkField("array_field", schema, Schema.nullableOf(Schema.arrayOf(Schema.unionOf(
      Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL)))));
    checkField("array_field_null", schema, Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.NULL))));
    checkField("array_field_empty", schema, Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.NULL))));
  }

  private void checkField(String name, Schema schema, Schema fieldSchema) {
    Schema.Field field = schema.getField(name);
    Assert.assertNotNull(field);
    Assert.assertEquals(field.getSchema(), fieldSchema);
  }

}
