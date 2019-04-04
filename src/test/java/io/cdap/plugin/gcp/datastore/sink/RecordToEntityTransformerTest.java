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
package io.cdap.plugin.gcp.datastore.sink;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Value;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.datastore.sink.util.IndexStrategy;
import io.cdap.plugin.gcp.datastore.sink.util.SinkKeyType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for {@link RecordToEntityTransformer} class.
 */
public class RecordToEntityTransformerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testTransformAllTypesIndexStrategyAll() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("timestamp_field", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("blob_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("entity_field",
                      Schema.nullableOf(Schema.recordOf("entity_field",
                        Schema.Field.of("nested_string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                        Schema.Field.of("nested_long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
      ))),
      Schema.Field.of("array_field", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG)))),
      Schema.Field.of("union_field", Schema.unionOf(
        Schema.of(Schema.Type.LONG),
        Schema.of(Schema.Type.STRING)
      ))
    );

    ZonedDateTime dateTime = ZonedDateTime.now();
    List<Long> longList = Arrays.asList(1L, null, 2L, null, 3L);

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "string_value")
      .set("long_field", 10L)
      .set("int_field", 15)
      .set("double_field", 10.5D)
      .set("float_field", 15.5F)
      .set("boolean_field", true)
      .setTimestamp("timestamp_field", dateTime)
      .set("blob_field", "test_blob".getBytes())
      .set("null_field", null)
      .set("entity_field", StructuredRecord.builder(schema.getField("entity_field").getSchema().getNonNullable())
        .set("nested_string_field", "nested_value")
        .set("nested_long_field", 20L)
        .build())
      .set("array_field", longList)
      .set("union_field", 2019L)
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.AUTO_GENERATED_KEY,
                                                                          "key",
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    FullEntity<?> outputEntity = transformer.transformStructuredRecord(inputRecord);

    Assert.assertEquals("string_value", outputEntity.getString("string_field"));
    Assert.assertFalse(outputEntity.getValue("string_field").excludeFromIndexes());

    Assert.assertEquals(10L, outputEntity.getLong("long_field"));
    Assert.assertFalse(outputEntity.getValue("long_field").excludeFromIndexes());

    Assert.assertEquals(15, outputEntity.getLong("int_field"));
    Assert.assertFalse(outputEntity.getValue("int_field").excludeFromIndexes());

    Assert.assertEquals(10.5D, outputEntity.getDouble("double_field"), 0);
    Assert.assertFalse(outputEntity.getValue("double_field").excludeFromIndexes());

    Assert.assertEquals(15.5, outputEntity.getDouble("float_field"), 0);
    Assert.assertFalse(outputEntity.getValue("float_field").excludeFromIndexes());

    Assert.assertEquals(Timestamp.ofTimeSecondsAndNanos(dateTime.toEpochSecond(), dateTime.getNano()),
                        outputEntity.getTimestamp("timestamp_field"));
    Assert.assertFalse(outputEntity.getValue("timestamp_field").excludeFromIndexes());

    Assert.assertTrue(outputEntity.getBoolean("boolean_field"));
    Assert.assertFalse(outputEntity.getValue("boolean_field").excludeFromIndexes());

    Assert.assertEquals("test_blob", new String(outputEntity.getBlob("blob_field").toByteArray()));
    Assert.assertFalse(outputEntity.getValue("blob_field").excludeFromIndexes());

    Assert.assertNull(outputEntity.getString("null_field"));
    Assert.assertFalse(outputEntity.getValue("null_field").excludeFromIndexes());

    FullEntity<IncompleteKey> nestedEntity = outputEntity.getEntity("entity_field");
    Assert.assertFalse(outputEntity.getValue("entity_field").excludeFromIndexes());

    Assert.assertEquals("nested_value", nestedEntity.getString("nested_string_field"));
    Assert.assertFalse(nestedEntity.getValue("nested_string_field").excludeFromIndexes());

    Assert.assertEquals(20L, nestedEntity.getLong("nested_long_field"));
    Assert.assertFalse(nestedEntity.getValue("nested_long_field").excludeFromIndexes());

    List<Long> actualLongList = outputEntity.getList("array_field").stream()
      .map(Value::get)
      .map(Long.class::cast)
      .collect(Collectors.toList());

    Assert.assertEquals(longList, actualLongList);
    Assert.assertFalse(outputEntity.getValue("array_field").excludeFromIndexes());

    Assert.assertEquals(2019L, outputEntity.getLong("union_field"));
    Assert.assertFalse(outputEntity.getValue("union_field").excludeFromIndexes());
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testTransformAllTypesIndexStrategyNone() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("timestamp_field", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("blob_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("null_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("entity_field",
                      Schema.nullableOf(Schema.recordOf("entity_field",
                        Schema.Field.of("nested_string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                        Schema.Field.of("nested_long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))))),
      Schema.Field.of("array_field", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG)))),
      Schema.Field.of("union_field", Schema.unionOf(
        Schema.of(Schema.Type.LONG),
        Schema.of(Schema.Type.STRING)
      ))
    );

    ZonedDateTime dateTime = ZonedDateTime.now();
    List<Long> longList = Arrays.asList(1L, null, 2L, null, 3L);

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "string_value")
      .set("long_field", 10L)
      .set("int_field", 15)
      .set("double_field", 10.5D)
      .set("float_field", 15.5F)
      .set("boolean_field", true)
      .setTimestamp("timestamp_field", dateTime)
      .set("blob_field", "test_blob".getBytes())
      .set("null_field", null)
      .set("entity_field", StructuredRecord.builder(schema.getField("entity_field").getSchema().getNonNullable())
        .set("nested_string_field", "nested_value")
        .set("nested_long_field", 20L)
        .build())
      .set("array_field", longList)
      .set("union_field", "union_string_value")
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.AUTO_GENERATED_KEY,
                                                                          "key",
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.NONE,
                                                                          Collections.emptySet());

    FullEntity<?> outputEntity = transformer.transformStructuredRecord(inputRecord);

    Assert.assertEquals("string_value", outputEntity.getString("string_field"));
    Assert.assertTrue(outputEntity.getValue("string_field").excludeFromIndexes());

    Assert.assertEquals(10L, outputEntity.getLong("long_field"));
    Assert.assertTrue(outputEntity.getValue("long_field").excludeFromIndexes());

    Assert.assertEquals(15, outputEntity.getLong("int_field"));
    Assert.assertTrue(outputEntity.getValue("int_field").excludeFromIndexes());

    Assert.assertEquals(10.5D, outputEntity.getDouble("double_field"), 0);
    Assert.assertTrue(outputEntity.getValue("double_field").excludeFromIndexes());

    Assert.assertEquals(15.5, outputEntity.getDouble("float_field"), 0);
    Assert.assertTrue(outputEntity.getValue("float_field").excludeFromIndexes());

    Assert.assertEquals(Timestamp.ofTimeSecondsAndNanos(dateTime.toEpochSecond(), dateTime.getNano()),
                        outputEntity.getTimestamp("timestamp_field"));
    Assert.assertTrue(outputEntity.getValue("timestamp_field").excludeFromIndexes());

    Assert.assertTrue(outputEntity.getBoolean("boolean_field"));
    Assert.assertTrue(outputEntity.getValue("boolean_field").excludeFromIndexes());

    Assert.assertEquals("test_blob", new String(outputEntity.getBlob("blob_field").toByteArray()));
    Assert.assertTrue(outputEntity.getValue("blob_field").excludeFromIndexes());

    Assert.assertNull(outputEntity.getString("null_field"));
    Assert.assertTrue(outputEntity.getValue("null_field").excludeFromIndexes());

    FullEntity<IncompleteKey> nestedEntity = outputEntity.getEntity("entity_field");
    Assert.assertTrue(outputEntity.getValue("entity_field").excludeFromIndexes());

    Assert.assertEquals("nested_value", nestedEntity.getString("nested_string_field"));
    Assert.assertTrue(nestedEntity.getValue("nested_string_field").excludeFromIndexes());

    Assert.assertEquals(20L, nestedEntity.getLong("nested_long_field"));
    Assert.assertTrue(nestedEntity.getValue("nested_long_field").excludeFromIndexes());

    List<Long> actualLongList = outputEntity.getList("array_field").stream()
      .map(Value::get)
      .map(Long.class::cast)
      .collect(Collectors.toList());

    Assert.assertEquals(longList, actualLongList);
    Assert.assertFalse(outputEntity.getValue("array_field").excludeFromIndexes());

    Assert.assertEquals("union_string_value", outputEntity.getString("union_field"));
    Assert.assertTrue(outputEntity.getValue("union_field").excludeFromIndexes());
  }

  @Test
  public void testTransformIndexStrategyCustom() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("long_field", Schema.of(Schema.Type.LONG)));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "string_value")
      .set("long_field", 200L)
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.AUTO_GENERATED_KEY,
                                                                          "key",
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.CUSTOM,
                                                                          Collections.singleton("string_field"));

    FullEntity<?> outputEntity = transformer.transformStructuredRecord(inputRecord);

    Assert.assertEquals("string_value", outputEntity.getString("string_field"));
    Assert.assertFalse(outputEntity.getValue("string_field").excludeFromIndexes());

    Assert.assertEquals(200L, outputEntity.getLong("long_field"));
    Assert.assertTrue(outputEntity.getValue("long_field").excludeFromIndexes());
  }

  @Test
  public void testTransformWithCustomStringKey() {
    String keyAlias = "key";

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of(keyAlias, Schema.of(Schema.Type.STRING)));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "string_value")
      .set(keyAlias, "custom_string_key_value")
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.CUSTOM_NAME,
                                                                          keyAlias,
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    FullEntity<?> outputEntity = transformer.transformStructuredRecord(inputRecord);

    Assert.assertEquals("string_value", outputEntity.getString("string_field"));

    Key expectedKey = Key.newBuilder(DatastoreSinkConfigHelper.TEST_PROJECT,
                                     DatastoreSinkConfigHelper.TEST_KIND,
                                     "custom_string_key_value")
      .setNamespace(DatastoreSinkConfigHelper.TEST_NAMESPACE)
      .build();
    Assert.assertEquals(expectedKey, outputEntity.getKey());
  }

  @Test
  public void testTransformWithCustomLongKey() {
    String keyAlias = "key";

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of(keyAlias, Schema.of(Schema.Type.LONG)));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "string_value")
      .set(keyAlias, 380L)
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.CUSTOM_NAME,
                                                                          keyAlias,
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    FullEntity<?> outputEntity = transformer.transformStructuredRecord(inputRecord);

    Assert.assertEquals("string_value", outputEntity.getString("string_field"));

    Key expectedKey = Key.newBuilder(DatastoreSinkConfigHelper.TEST_PROJECT,
                                     DatastoreSinkConfigHelper.TEST_KIND,
                                     380L)
      .setNamespace(DatastoreSinkConfigHelper.TEST_NAMESPACE)
      .build();
    Assert.assertEquals(expectedKey, outputEntity.getKey());
  }

  @Test
  public void testTransformWithCustomLongKeyWithAncestor() {
    String keyAlias = "key";

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of(keyAlias, Schema.of(Schema.Type.LONG)));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "string_value")
      .set(keyAlias, 380L)
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.CUSTOM_NAME,
                                                                          keyAlias,
                                                                          Collections.singletonList(
                                                                            PathElement.of("A", 100)),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    FullEntity<?> outputEntity = transformer.transformStructuredRecord(inputRecord);

    Assert.assertEquals("string_value", outputEntity.getString("string_field"));

    Key expectedKey = new KeyFactory(DatastoreSinkConfigHelper.TEST_PROJECT)
      .setKind(DatastoreSinkConfigHelper.TEST_KIND)
      .setNamespace(DatastoreSinkConfigHelper.TEST_NAMESPACE)
      .addAncestor(PathElement.of("A", 100))
      .newKey(380L);
    Assert.assertEquals(expectedKey, outputEntity.getKey());
  }

  @Test
  public void testTransformWithKeyLiteral() {
    String keyAlias = "key";

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of(keyAlias, Schema.of(Schema.Type.STRING)));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "string_value")
      .set(keyAlias, String.format("key(A, 100, %s, 'test_string_id')", DatastoreSinkConfigHelper.TEST_KIND))
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.KEY_LITERAL,
                                                                          keyAlias,
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    FullEntity<?> outputEntity = transformer.transformStructuredRecord(inputRecord);

    Assert.assertEquals("string_value", outputEntity.getString("string_field"));

    Key expectedKey = new KeyFactory(DatastoreSinkConfigHelper.TEST_PROJECT)
      .setKind(DatastoreSinkConfigHelper.TEST_KIND)
      .setNamespace(DatastoreSinkConfigHelper.TEST_NAMESPACE)
      .addAncestor(PathElement.of("A", 100))
      .newKey("test_string_id");
    Assert.assertEquals(expectedKey, outputEntity.getKey());
  }

  @Test
  public void testTransformWithUrlSafeKey() {
    String keyAlias = "key";

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of(keyAlias, Schema.of(Schema.Type.STRING)));

    Key expectedKey = new KeyFactory(DatastoreSinkConfigHelper.TEST_PROJECT)
      .setKind(DatastoreSinkConfigHelper.TEST_KIND)
      .setNamespace(DatastoreSinkConfigHelper.TEST_NAMESPACE)
      .addAncestor(PathElement.of("A", 100))
      .newKey("test_string_id");

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "string_value")
      .set(keyAlias, expectedKey.toUrlSafe())
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.URL_SAFE_KEY,
                                                                          keyAlias,
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    FullEntity<?> outputEntity = transformer.transformStructuredRecord(inputRecord);

    Assert.assertEquals("string_value", outputEntity.getString("string_field"));
    Assert.assertEquals(expectedKey, outputEntity.getKey());
  }

  @Test
  public void testTransformArrayEmpty() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("array_field", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("array_field", Collections.emptyList())
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.AUTO_GENERATED_KEY,
                                                                          "id",
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    FullEntity<?> outputEntity = transformer.transformStructuredRecord(inputRecord);

    List<Value<Boolean>> actual = outputEntity.getList("array_field");

    List<Boolean> actualList = actual.stream()
      .map(Value::get)
      .collect(Collectors.toList());

    Assert.assertEquals(Collections.emptyList(), actualList);
  }

  @Test
  public void testTransformArrayFromArray() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("array_field", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))));

    String[] stringArray = new String[] {"A", "B", "C", null};

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("array_field", stringArray)
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.AUTO_GENERATED_KEY,
                                                                          "id",
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    FullEntity<?> outputEntity = transformer.transformStructuredRecord(inputRecord);

    List<Value<String>> actual = outputEntity.getList("array_field");

    List<String> actualList = actual.stream()
      .map(Value::get)
      .collect(Collectors.toList());

    Assert.assertEquals(Arrays.asList(stringArray), actualList);
  }

  @Test
  public void testTransformComplexUnion() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("union_field", Schema.unionOf(
        Schema.of(Schema.Type.STRING),
        Schema.of(Schema.Type.BOOLEAN),
        Schema.of(Schema.Type.NULL)
      )));

    StructuredRecord inputRecordS = StructuredRecord.builder(schema)
      .set("union_field", "a")
      .build();

    StructuredRecord inputRecordB = StructuredRecord.builder(schema)
      .set("union_field", true)
      .build();

    StructuredRecord inputRecordN = StructuredRecord.builder(schema)
      .set("union_field", null)
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.AUTO_GENERATED_KEY,
                                                                          "key",
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    FullEntity<?> outputEntityS = transformer.transformStructuredRecord(inputRecordS);
    Assert.assertEquals("a", outputEntityS.getString("union_field"));

    FullEntity<?> outputEntityB = transformer.transformStructuredRecord(inputRecordB);
    Assert.assertTrue(outputEntityB.getBoolean("union_field"));

    FullEntity<?> outputEntityN = transformer.transformStructuredRecord(inputRecordN);
    Value<?> actualN = outputEntityN.getValue("union_field");
    Assert.assertNull(actualN.get());
  }

  @Test
  public void testTransformUndeclaredUnionValue() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("union_field", Schema.unionOf(
                                      Schema.of(Schema.Type.STRING),
                                      Schema.of(Schema.Type.BOOLEAN)
                                    )));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("union_field", 1L)
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.AUTO_GENERATED_KEY,
                                                                          "key",
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    thrown.expect(IllegalStateException.class);

    transformer.transformStructuredRecord(inputRecord);
  }

  @Test
  public void testTransformDifferentType() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("field", Schema.of(Schema.Type.LONG)));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("field", "string_value")
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.AUTO_GENERATED_KEY,
                                                                          "key",
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    thrown.expect(UnexpectedFormatException.class);

    transformer.transformStructuredRecord(inputRecord);
  }

  @Test
  public void testTransformNullableKeyField() {
    String keyAlias = "key";

    Schema schema = Schema.recordOf("schema",
      Schema.Field.of(keyAlias, Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set(keyAlias, null)
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.CUSTOM_NAME,
                                                                          keyAlias,
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    thrown.expect(IllegalStateException.class);

    transformer.transformStructuredRecord(inputRecord);
  }

}
