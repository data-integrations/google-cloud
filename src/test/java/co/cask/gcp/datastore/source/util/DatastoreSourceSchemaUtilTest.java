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
package co.cask.gcp.datastore.source.util;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.datastore.source.DatastoreSourceConfig;
import co.cask.gcp.datastore.source.DatastoreSourceConfigHelper;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.LatLng;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for schema utility methods.
 */
public class DatastoreSourceSchemaUtilTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testValidateKeyAliasValidKey() {
    String keyName = "key";
    Schema schema = Schema.recordOf("schema", Schema.Field.of(keyName, Schema.of(Schema.Type.STRING)));
    DatastoreSourceSchemaUtil.validateKeyAlias(schema, keyName);
  }

  @Test
  public void testValidateKeyAliasNullableKey() {
    String keyName = "key";
    Schema schema = Schema.recordOf("schema", Schema.Field.of(keyName,
      Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Key field [%s] type must be non-nullable STRING", keyName));
    DatastoreSourceSchemaUtil.validateKeyAlias(schema, keyName);
  }

  @Test
  public void testValidateKeyAliasIncorrectType() {
    String keyName = "key";
    Schema schema = Schema.recordOf("schema", Schema.Field.of(keyName, Schema.of(Schema.Type.INT)));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Key field [%s] type must be non-nullable STRING", keyName));
    DatastoreSourceSchemaUtil.validateKeyAlias(schema, keyName);
  }

  @Test
  public void testValidateKeyAliasAbsentKey() {
    String keyName = "key";
    Schema schema = Schema.recordOf("schema", Schema.Field.of("id", Schema.of(Schema.Type.LONG)));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Key field [%s] must exist in the schema", keyName));
    DatastoreSourceSchemaUtil.validateKeyAlias(schema, keyName);
  }

  @Test
  public void testValidateSchemaAcceptedTypes() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("long_field", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("double_field", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("timestamp_field", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("blob_field", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("record_field",
        Schema.recordOf("record",
          Schema.Field.of("record_string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
          Schema.Field.of("record_long_field", Schema.of(Schema.Type.LONG))))
    );
    DatastoreSourceSchemaUtil.validateSchema(schema);
  }

  @Test
  public void testValidateSchemaUnsupportedTypeINT() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field", Schema.of(Schema.Type.INT)));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported type: " + Schema.Type.INT);

    DatastoreSourceSchemaUtil.validateSchema(schema);
  }

  @Test
  public void testValidateSchemaInt() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field", Schema.of(Schema.Type.INT)));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported type: " + Schema.Type.INT);

    DatastoreSourceSchemaUtil.validateSchema(schema);
  }

  @Test
  public void testValidateSchemaNull() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field", Schema.of(Schema.Type.NULL)));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported type: " + Schema.Type.NULL);

    DatastoreSourceSchemaUtil.validateSchema(schema);
  }

  @Test
  public void testValidateSchemaEnum() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field", Schema.enumWith("one", "two")));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported type: " + Schema.Type.ENUM);

    DatastoreSourceSchemaUtil.validateSchema(schema);
  }

  @Test
  public void testValidateSchemaArray() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field",
      Schema.arrayOf(Schema.of(Schema.Type.INT))));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported type: " + Schema.Type.ARRAY);

    DatastoreSourceSchemaUtil.validateSchema(schema);
  }

  @Test
  public void testValidateSchemaMap() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field", Schema.mapOf(
      Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING))));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported type: " + Schema.Type.MAP);

    DatastoreSourceSchemaUtil.validateSchema(schema);
  }

  @Test
  public void testValidateSchemaUnsupportedLongLogicalType() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("field", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported logical type for Long: " + Schema.LogicalType.TIMESTAMP_MILLIS);

    DatastoreSourceSchemaUtil.validateSchema(schema);
  }

  @Test
  public void testValidateSchemaComplexUnion() {
    Schema schema = Schema.recordOf("schema", Schema.Field.of("field",
      Schema.nullableOf(Schema.unionOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.LONG)))));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Complex UNION type is not supported");

    DatastoreSourceSchemaUtil.validateSchema(schema);
  }

  @Test
  public void testConstructSchema() {
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

    Schema schema = DatastoreSourceSchemaUtil.constructSchema(entity,
      DatastoreSourceConfigHelper.newConfigBuilder().setKeyType(SourceKeyType.NONE.getValue()).build());

    List<Schema.Field> fields = schema.getFields();
    assertNotNull(fields);
    assertEquals(8, fields.size());
    checkSimpleField("string_field", schema, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    checkSimpleField("long_field", schema, Schema.nullableOf(Schema.of(Schema.Type.LONG)));
    checkSimpleField("double_field", schema, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)));
    checkSimpleField("boolean_field", schema, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)));
    checkSimpleField("timestamp_field", schema, Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));
    checkSimpleField("blob_field", schema, Schema.nullableOf(Schema.of(Schema.Type.BYTES)));
    checkSimpleField("null_field", schema, Schema.nullableOf(Schema.of(Schema.Type.STRING)));

    Schema.Field entityField = schema.getField("entity_field");
    assertNotNull(entityField);
    assertTrue(entityField.getSchema().isNullable());
    Schema entitySchema = entityField.getSchema().getNonNullable();
    assertEquals(Schema.Type.RECORD, schema.getType());
    List<Schema.Field> entityFields = entitySchema.getFields();
    assertNotNull(entityFields);
    assertEquals(2, entityFields.size());
    checkSimpleField("nested_string_field", entitySchema, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    checkSimpleField("nested_long_field", entitySchema, Schema.nullableOf(Schema.of(Schema.Type.LONG)));

    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setKeyAlias("key")
      .build();

    Schema schemaWithKey = DatastoreSourceSchemaUtil.constructSchema(entity, config);
    List<Schema.Field> schemaWithKeyFields = schemaWithKey.getFields();
    assertNotNull(schemaWithKeyFields);
    assertEquals(9, schemaWithKeyFields.size());
    checkSimpleField("key", schemaWithKey, Schema.of(Schema.Type.STRING));
  }

  private void checkSimpleField(String name, Schema schema, Schema fieldSchema) {
    Schema.Field field = schema.getField(name);
    assertNotNull(field);
    assertEquals(field.getSchema(), fieldSchema);
  }

}
