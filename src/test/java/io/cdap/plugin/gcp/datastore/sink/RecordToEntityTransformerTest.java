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

import com.google.common.primitives.Ints;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.protobuf.TextFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.datastore.sink.util.IndexStrategy;
import io.cdap.plugin.gcp.datastore.sink.util.SinkKeyType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
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

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);

    Value value = outputEntity.getPropertiesOrThrow("string_field");
    Assert.assertEquals("string_value", DatastoreHelper.getString(value));
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("long_field");
    Assert.assertEquals(10L, DatastoreHelper.getLong(value));
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("int_field");
    Assert.assertEquals(15,  DatastoreHelper.getLong(value));
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("double_field");
    Assert.assertEquals(10.5D, DatastoreHelper.getDouble(value), 0);
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("float_field");
    Assert.assertEquals(15.5, DatastoreHelper.getDouble(value), 0);
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("timestamp_field");
    Assert.assertEquals(dateTime.toInstant().toEpochMilli() * 1000,
                        DatastoreHelper.getTimestamp(value));
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("boolean_field");
    Assert.assertTrue(DatastoreHelper.getBoolean(value));
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("blob_field");
    Assert.assertEquals("test_blob", new String(DatastoreHelper.getByteString(value).toByteArray()));
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("null_field");
    Assert.assertEquals(value.getValueTypeCase(), Value.ValueTypeCase.NULL_VALUE);
    Assert.assertFalse(value.getExcludeFromIndexes());

    //FullEntity<IncompleteKey> nestedEntity = outputEntity.getEntity("entity_field");
    value = outputEntity.getPropertiesOrThrow("entity_field");
    Assert.assertFalse(value.getExcludeFromIndexes());
    Entity nestedEntity = DatastoreHelper.getEntity(value);
    value = nestedEntity.getPropertiesOrThrow("nested_string_field");
    Assert.assertEquals("nested_value", DatastoreHelper.getString(value));
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = nestedEntity.getPropertiesOrThrow("nested_long_field");
    Assert.assertEquals(20L, DatastoreHelper.getLong(value));
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("array_field");
    List<Long> actualLongList = DatastoreHelper.getList(value).stream()
      .map(v -> v.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE ? DatastoreHelper.getLong(v) : null)
      .map(Long.class::cast)
      .collect(Collectors.toList());

    Assert.assertEquals(longList, actualLongList);
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("union_field");
    Assert.assertEquals(2019L, DatastoreHelper.getLong(value));
    Assert.assertFalse(value.getExcludeFromIndexes());
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
      Schema.Field.of("datetime_field", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))),
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
    LocalDateTime localDateTime = LocalDateTime.now();
    List<Long> longList = Arrays.asList(1L, null, 2L, null, 3L);

    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "string_value")
      .set("long_field", 10L)
      .set("int_field", 15)
      .set("double_field", 10.5D)
      .set("float_field", 15.5F)
      .set("boolean_field", true)
      .setTimestamp("timestamp_field", dateTime)
      .setDateTime("datetime_field", localDateTime)
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

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);

    Value value = outputEntity.getPropertiesOrThrow("string_field");
    Assert.assertEquals("string_value", DatastoreHelper.getString(value));
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("long_field");
    Assert.assertEquals(10L, DatastoreHelper.getLong(value));
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("int_field");
    Assert.assertEquals(15, DatastoreHelper.getLong(value));
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("double_field");
    Assert.assertEquals(10.5D, DatastoreHelper.getDouble(value), 0);
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("float_field");
    Assert.assertEquals(15.5, DatastoreHelper.getDouble(value), 0);
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("timestamp_field");
    Assert.assertEquals(dateTime.toInstant().toEpochMilli() * 1000,
                        DatastoreHelper.getTimestamp(value));
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("datetime_field");
    Assert.assertEquals(localDateTime.toString(), DatastoreHelper.getString(value));
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("boolean_field");
    Assert.assertTrue(DatastoreHelper.getBoolean(value));
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("blob_field");
    Assert.assertEquals("test_blob", new String(DatastoreHelper.getByteString(value).toByteArray()));
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("null_field");
    Assert.assertEquals(value.getValueTypeCase(), Value.ValueTypeCase.NULL_VALUE);
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("entity_field");
    Assert.assertTrue(value.getExcludeFromIndexes());
    Entity nestedEntity = DatastoreHelper.getEntity(value);
    value = nestedEntity.getPropertiesOrThrow("nested_string_field");
    Assert.assertEquals("nested_value", DatastoreHelper.getString(value));
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = nestedEntity.getPropertiesOrThrow("nested_long_field");
    Assert.assertEquals(20L, DatastoreHelper.getLong(value));
    Assert.assertTrue(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("array_field");
    List<Long> actualLongList = DatastoreHelper.getList(value).stream()
      .map(v -> v.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE ? DatastoreHelper.getLong(v) : null)
      .map(Long.class::cast)
      .collect(Collectors.toList());

    Assert.assertEquals(longList, actualLongList);
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("union_field");
    Assert.assertEquals("union_string_value", DatastoreHelper.getString(value));
    Assert.assertTrue(value.getExcludeFromIndexes());

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

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);
    Value value = outputEntity.getPropertiesOrThrow("string_field");

    Assert.assertEquals("string_value", DatastoreHelper.getString(value));
    Assert.assertFalse(value.getExcludeFromIndexes());

    value = outputEntity.getPropertiesOrThrow("long_field");
    Assert.assertEquals(200L, DatastoreHelper.getLong(value));
    Assert.assertTrue(value.getExcludeFromIndexes());
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

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);
    Value value = outputEntity.getPropertiesOrThrow("string_field");

    Assert.assertEquals("string_value", DatastoreHelper.getString(value));

    Key expectedKey = Key.newBuilder()
      .setPartitionId(PartitionId.newBuilder()
                        .setProjectId(DatastoreSinkConfigHelper.TEST_PROJECT)
                        .setNamespaceId(DatastoreSinkConfigHelper.TEST_NAMESPACE))
      .addPath(Key.PathElement.newBuilder()
                 .setName("custom_string_key_value")
                 .setKind(DatastoreSinkConfigHelper.TEST_KIND))
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

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);
    Value value = outputEntity.getPropertiesOrThrow("string_field");

    Assert.assertEquals("string_value", DatastoreHelper.getString(value));

    Key expectedKey = Key.newBuilder()
      .setPartitionId(PartitionId.newBuilder()
                        .setProjectId(DatastoreSinkConfigHelper.TEST_PROJECT)
                        .setNamespaceId(DatastoreSinkConfigHelper.TEST_NAMESPACE))
      .addPath(Key.PathElement.newBuilder()
                 .setId(380L)
                 .setKind(DatastoreSinkConfigHelper.TEST_KIND))
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

    Key.PathElement pathElement = Key.PathElement.newBuilder()
      .setKind("A")
      .setId(100)
      .build();
    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.CUSTOM_NAME,
                                                                          keyAlias,
                                                                          Collections.singletonList(pathElement),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);
    Value value = outputEntity.getPropertiesOrThrow("string_field");

    Assert.assertEquals("string_value", DatastoreHelper.getString(value));

    Key expectedKey = Key.newBuilder()
      .setPartitionId(PartitionId.newBuilder()
                        .setProjectId(DatastoreSinkConfigHelper.TEST_PROJECT)
                        .setNamespaceId(DatastoreSinkConfigHelper.TEST_NAMESPACE))
      .addPath(Key.PathElement.newBuilder()
                 .setId(100)
                 .setKind("A"))
      .addPath(Key.PathElement.newBuilder()
                 .setId(380L)
                 .setKind(DatastoreSinkConfigHelper.TEST_KIND))
      .build();
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

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);

    Value value = outputEntity.getPropertiesOrThrow("string_field");
    Assert.assertEquals("string_value", DatastoreHelper.getString(value));

    Key expectedKey = Key.newBuilder()
      .setPartitionId(PartitionId.newBuilder()
                        .setProjectId(DatastoreSinkConfigHelper.TEST_PROJECT)
                        .setNamespaceId(DatastoreSinkConfigHelper.TEST_NAMESPACE))
      .addPath(Key.PathElement.newBuilder()
                 .setId(100)
                 .setKind("A"))
      .addPath(Key.PathElement.newBuilder()
                 .setName("test_string_id")
                 .setKind(DatastoreSinkConfigHelper.TEST_KIND))
      .build();
    Assert.assertEquals(expectedKey, outputEntity.getKey());
  }

  @Test
  public void testTransformWithUrlSafeKey() {
    String keyAlias = "key";

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of(keyAlias, Schema.of(Schema.Type.STRING)));

    Key expectedKey = Key.newBuilder()
      .setPartitionId(PartitionId.newBuilder()
                        .setProjectId(DatastoreSinkConfigHelper.TEST_PROJECT)
                        .setNamespaceId(DatastoreSinkConfigHelper.TEST_NAMESPACE))
      .addPath(Key.PathElement.newBuilder()
                 .setId(100)
                 .setKind("A"))
      .addPath(Key.PathElement.newBuilder()
                 .setName("test_string_id")
                 .setKind(DatastoreSinkConfigHelper.TEST_KIND)).build();

    String urlSafeKey = "";
    try {
      urlSafeKey = URLEncoder.encode(TextFormat.printToString(expectedKey), StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      Assert.fail(String.format("URL encoding failed unexpectedly: %s", e.getMessage()));
    }
    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "string_value")
      .set(keyAlias, urlSafeKey)
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.URL_SAFE_KEY,
                                                                          keyAlias,
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);

    Value value = outputEntity.getPropertiesOrThrow("string_field");
    Assert.assertEquals("string_value", DatastoreHelper.getString(value));
    Assert.assertEquals(expectedKey, outputEntity.getKey());
  }

  @Test
  public void testTransformWithAutogeneratedKey() {
    String keyAlias = "key";

    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of(keyAlias, Schema.of(Schema.Type.STRING)));

    Key expectedKey = Key.newBuilder()
      .setPartitionId(PartitionId.newBuilder()
                        .setProjectId(DatastoreSinkConfigHelper.TEST_PROJECT)
                        .setNamespaceId(DatastoreSinkConfigHelper.TEST_NAMESPACE))
      .addPath(Key.PathElement.newBuilder()
                 .setKind("A")
                 .setId(100))
      .addPath(Key.PathElement.newBuilder()
                 .setKind(DatastoreSinkConfigHelper.TEST_KIND))
      .build();

    Key.PathElement pathElement = Key.PathElement.newBuilder()
      .setKind("A")
      .setId(100)
      .build();


    String urlSafeKey = "";
    try {
      urlSafeKey = URLEncoder.encode(TextFormat.printToString(expectedKey), StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      Assert.fail(String.format("URL encoding failed unexpectedly: %s", e.getMessage()));
    }
    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("string_field", "string_value")
      .set(keyAlias, urlSafeKey)
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.AUTO_GENERATED_KEY,
                                                                          keyAlias,
                                                                          Collections.singletonList(pathElement),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);

    Value value = outputEntity.getPropertiesOrThrow("string_field");
    Assert.assertEquals("string_value", DatastoreHelper.getString(value));
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

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);
    Value value = outputEntity.getPropertiesOrThrow("array_field");

    List<Boolean> actualList = DatastoreHelper.getList(value).stream()
      .map(DatastoreHelper::getBoolean)
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

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);
    Value value = outputEntity.getPropertiesOrThrow("array_field");

    List<String> actualList = DatastoreHelper.getList(value).stream()
      .map(v -> v.getValueTypeCase() == Value.ValueTypeCase.STRING_VALUE ? DatastoreHelper.getString(v) : null)
      .collect(Collectors.toList());

    Assert.assertEquals(Arrays.asList(stringArray), actualList);
  }

  @Test
  public void testTransformIntArray() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("array_field",
                                                    Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.INT)))));
    int[] intArray = new int[] {1, 2, 3};
    StructuredRecord inputRecord = StructuredRecord.builder(schema)
      .set("array_field", intArray)
      .build();

    RecordToEntityTransformer transformer = new RecordToEntityTransformer(DatastoreSinkConfigHelper.TEST_PROJECT,
                                                                          DatastoreSinkConfigHelper.TEST_NAMESPACE,
                                                                          DatastoreSinkConfigHelper.TEST_KIND,
                                                                          SinkKeyType.AUTO_GENERATED_KEY,
                                                                          "id",
                                                                          Collections.emptyList(),
                                                                          IndexStrategy.ALL,
                                                                          Collections.emptySet());

    Entity outputEntity = transformer.transformStructuredRecord(inputRecord);
    Value value = outputEntity.getPropertiesOrThrow("array_field");

    List<Integer> actualList = DatastoreHelper.getList(value).stream()
      .map(DatastoreHelper::getLong)
      .map(Long::intValue)
      .collect(Collectors.toList());
    Assert.assertEquals(Ints.asList(intArray), actualList);
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

    Entity outputEntityS = transformer.transformStructuredRecord(inputRecordS);
    Value value = outputEntityS.getPropertiesOrThrow("union_field");
    Assert.assertEquals("a", DatastoreHelper.getString(value));

    Entity outputEntityB = transformer.transformStructuredRecord(inputRecordB);
    value = outputEntityB.getPropertiesOrThrow("union_field");
    Assert.assertTrue(DatastoreHelper.getBoolean(value));

    Entity outputEntityN = transformer.transformStructuredRecord(inputRecordN);
    value = outputEntityN.getPropertiesOrThrow("union_field");
    Assert.assertEquals(Value.ValueTypeCase.NULL_VALUE, value.getValueTypeCase());
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
