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
package co.cask.gcp.datastore.source;

import co.cask.cdap.api.data.schema.Schema;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.LatLng;
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
    checkSimpleField("string_field", schemaWithKey, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    checkSimpleField("key", schemaWithKey, Schema.of(Schema.Type.STRING));
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
    Assert.assertEquals(8, fields.size());
    checkSimpleField("string_field", schema, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    checkSimpleField("long_field", schema, Schema.nullableOf(Schema.of(Schema.Type.LONG)));
    checkSimpleField("double_field", schema, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)));
    checkSimpleField("boolean_field", schema, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)));
    checkSimpleField("timestamp_field", schema, Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));
    checkSimpleField("blob_field", schema, Schema.nullableOf(Schema.of(Schema.Type.BYTES)));
    checkSimpleField("null_field", schema, Schema.nullableOf(Schema.of(Schema.Type.STRING)));

    Schema.Field entityField = schema.getField("entity_field");
    Assert.assertNotNull(entityField);
    Assert.assertTrue(entityField.getSchema().isNullable());
    Schema entitySchema = entityField.getSchema().getNonNullable();
    Assert.assertEquals(Schema.Type.RECORD, schema.getType());
    List<Schema.Field> entityFields = entitySchema.getFields();
    Assert.assertNotNull(entityFields);
    Assert.assertEquals(2, entityFields.size());
    checkSimpleField("nested_string_field", entitySchema, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    checkSimpleField("nested_long_field", entitySchema, Schema.nullableOf(Schema.of(Schema.Type.LONG)));
  }

  private void checkSimpleField(String name, Schema schema, Schema fieldSchema) {
    Schema.Field field = schema.getField(name);
    Assert.assertNotNull(field);
    Assert.assertEquals(field.getSchema(), fieldSchema);
  }

}
