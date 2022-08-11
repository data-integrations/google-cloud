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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Arrays;
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

    Schema schemaWithKey = datastoreSource.constructSchema(entity, true, "key");

    List<Schema.Field> schemaWithKeyFields = schemaWithKey.getFields();
    Assert.assertNotNull(schemaWithKeyFields);
    Assert.assertEquals(2, schemaWithKeyFields.size());
    checkField("string_field", schemaWithKey, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    checkField("key", schemaWithKey, Schema.of(Schema.Type.STRING));
  }

  @Test
  public void testGetSchemaIsIncludeKeyFalse() {
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
    ArrayValue arrayFieldValue = ArrayValue.newBuilder().addAllValues(Arrays.asList(
      Value.newBuilder().setIntegerValue(10).build(),
      Value.newBuilder().setIntegerValue(20).build(),
      Value.newBuilder().setStringValue("string_value_1").build(),
      Value.newBuilder().setStringValue("string_value_1").build()))
      .build();

    Value nullValue = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    ArrayValue arrayNullValue = ArrayValue.newBuilder().addAllValues(Arrays.asList(
      nullValue, nullValue)).build();

    ArrayValue emptyValue = ArrayValue.newBuilder().build();

      Entity entity = Entity.newBuilder()
      .setKey(Key.newBuilder()
                .setPartitionId(PartitionId.newBuilder()
                                  .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT))
                .addPath(Key.PathElement.newBuilder()
                           .setId(1)
                           .setKind(DatastoreSourceConfigHelper.TEST_KIND).build())
                .build())
      .putProperties("array_field", Value.newBuilder().setArrayValue(arrayFieldValue).build())
        .putProperties("array_field_null", Value.newBuilder().setArrayValue(arrayNullValue).build())
        .putProperties("array_field_empty", Value.newBuilder().setArrayValue(emptyValue).build())
        .build();

      Schema schema = datastoreSource.constructSchema(entity, false, "key");
    checkField("array_field", schema, Schema.nullableOf(Schema.arrayOf(Schema.unionOf(
      Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL)))));
    checkField("array_field_null", schema, Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.NULL))));
    checkField("array_field_empty", schema, Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.NULL))));
  }

  @Test
  public void testConstructAncestorWithIdKey() {
    String ancestor = "Key(A,100,B,'bId',`C C C`, 123)";

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder().setAncestor(ancestor).build();
    Key key = datastoreSource.constructAncestorKey(config, collector);
    Assert.assertEquals(config.getProject(), key.getPartitionId().getProjectId());
    Assert.assertEquals(config.getNamespace(), key.getPartitionId().getNamespaceId());
    Assert.assertEquals(config.getAncestor(collector), key.getPathList().subList(0, key.getPathCount()));
  }

  @Test
  public void testConstructAncestorWithNameKey() {
    String ancestor = "Key(A,100,B,'bId',`C C C`, 'cId')";

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder().setAncestor(ancestor).build();
    Key key = datastoreSource.constructAncestorKey(config, collector);
    Assert.assertEquals(config.getProject(), key.getPartitionId().getProjectId());
    Assert.assertEquals(config.getNamespace(), key.getPartitionId().getNamespaceId());
    Assert.assertEquals(config.getAncestor(collector), key.getPathList().subList(0, key.getPathCount()));
  }

  @Test
  public void testConstructAncestorWithNoAncestor() {
    String ancestor = "Key(`C C C`, 'cId')";
    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder().setAncestor(ancestor).build();
    Assert.assertNull(datastoreSource.constructAncestorKey(config, collector));
  }

  private void checkField(String name, Schema schema, Schema fieldSchema) {
    Schema.Field field = schema.getField(name);
    Assert.assertNotNull(field);
    Assert.assertEquals(field.getSchema(), fieldSchema);
  }

}
