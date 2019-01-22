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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.gcp.datastore.source.util.SourceKeyType;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.LatLng;
import org.apache.hadoop.io.NullWritable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DatastoreSource} class.
 */
@RunWith(MockitoJUnitRunner.class)
public class DatastoreSourceTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @InjectMocks
  private DatastoreSource datastoreSource;

  @Mock
  private DatastoreSourceConfig config;

  @Mock
  private KeyValue<NullWritable, Entity> input;

  @Mock
  private Emitter<StructuredRecord> emitter;

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testTransformAllTypes() throws Exception {
    when(config.isIncludeKey()).thenReturn(false);

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
          Schema.Field.of("nested_long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
      )))
    );

    when(config.getSchema()).thenReturn(schema);

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
    when(input.getValue()).thenReturn(entity);

    doNothing().when(emitter).emit(any(StructuredRecord.class));
    ArgumentCaptor<StructuredRecord> captor = ArgumentCaptor.forClass(StructuredRecord.class);

    datastoreSource.initialize(mock(BatchRuntimeContext.class));
    datastoreSource.transform(input, emitter);

    verify(emitter).emit(captor.capture());
    StructuredRecord record = captor.getValue();

    assertEquals("string_value", record.get("string_field"));
    assertEquals(10L, (long) record.get("long_field"));
    assertEquals(10.5D, record.get("double_field"), 0);

    ZonedDateTime recordTs = record.getTimestamp("timestamp_field");
    Timestamp actualTs = Timestamp.ofTimeSecondsAndNanos(recordTs.toEpochSecond(), recordTs.getNano());
    assertEquals(entityTs, actualTs);

    assertTrue(record.get("boolean_field"));
    assertEquals("test_blob", new String((byte[]) record.get("blob_field")));
    assertNull(record.get("null_field"));

    StructuredRecord entityRecord = record.get("entity_field");
    assertEquals("nested_value", entityRecord.get("nested_string_field"));
    assertEquals(20L, (long) entityRecord.get("nested_long_field"));
  }

  @Test
  public void testTransformWithKeyLiteral() throws Exception {
    when(config.isIncludeKey()).thenReturn(true);
    when(config.getKeyAlias()).thenReturn("key");
    when(config.getKeyType()).thenReturn(SourceKeyType.KEY_LITERAL);

    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("key", Schema.of(Schema.Type.STRING))
    );
    when(config.getSchema()).thenReturn(schema);

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("string_field", "string_value")
      .build();
    when(input.getValue()).thenReturn(entity);

    doNothing().when(emitter).emit(any(StructuredRecord.class));
    ArgumentCaptor<StructuredRecord> captor = ArgumentCaptor.forClass(StructuredRecord.class);

    datastoreSource.initialize(mock(BatchRuntimeContext.class));
    datastoreSource.transform(input, emitter);

    verify(emitter).emit(captor.capture());
    StructuredRecord record = captor.getValue();

    assertEquals("string_value", record.get("string_field"));
    assertEquals(String.format("key(%s, %s)", DatastoreSourceConfigHelper.TEST_KIND, 1), record.get("key"));
  }

  @Test
  public void testTransformWithUrlSafeKey() throws Exception {
    when(config.isIncludeKey()).thenReturn(true);
    when(config.getKeyAlias()).thenReturn("key");
    when(config.getKeyType()).thenReturn(SourceKeyType.URL_SAFE_KEY);

    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("key", Schema.of(Schema.Type.STRING))
    );
    when(config.getSchema()).thenReturn(schema);

    Key key = Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT, DatastoreSourceConfigHelper.TEST_KIND, 1)
      .build();
    Entity entity = Entity.newBuilder(key)
      .set("string_field", "string_value")
      .build();
    when(input.getValue()).thenReturn(entity);

    doNothing().when(emitter).emit(any(StructuredRecord.class));
    ArgumentCaptor<StructuredRecord> captor = ArgumentCaptor.forClass(StructuredRecord.class);

    datastoreSource.initialize(mock(BatchRuntimeContext.class));
    datastoreSource.transform(input, emitter);

    verify(emitter).emit(captor.capture());
    StructuredRecord record = captor.getValue();

    assertEquals("string_value", record.get("string_field"));
    assertEquals(key.toUrlSafe(), record.get("key"));
  }

  @Test
  public void testTransformWithoutKey() throws Exception {
    when(config.isIncludeKey()).thenReturn(false);
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING))
    );
    when(config.getSchema()).thenReturn(schema);

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("string_field", "string_value")
      .build();
    when(input.getValue()).thenReturn(entity);

    doNothing().when(emitter).emit(any(StructuredRecord.class));
    ArgumentCaptor<StructuredRecord> captor = ArgumentCaptor.forClass(StructuredRecord.class);

    datastoreSource.initialize(mock(BatchRuntimeContext.class));
    datastoreSource.transform(input, emitter);

    verify(emitter).emit(captor.capture());
    StructuredRecord record = captor.getValue();

    assertEquals("string_value", record.get("string_field"));
  }

  @Test
  public void testTransformNullIntoNotNull() throws Exception {
    when(config.isIncludeKey()).thenReturn(false);
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING))
    );
    when(config.getSchema()).thenReturn(schema);

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .setNull("string_field")
      .build();
    when(input.getValue()).thenReturn(entity);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Can not set null value to a not null field : string_field");

    datastoreSource.initialize(mock(BatchRuntimeContext.class));
    datastoreSource.transform(input, emitter);
  }

  @Test
  public void testTransformMissingField() throws Exception {
    when(config.isIncludeKey()).thenReturn(false);
    Schema schema = Schema.recordOf("schema",
       Schema.Field.of("string_field", Schema.of(Schema.Type.STRING)),
       Schema.Field.of("missing_field", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );
    when(config.getSchema()).thenReturn(schema);

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("string_field", "string_value")
      .build();
    when(input.getValue()).thenReturn(entity);

    doNothing().when(emitter).emit(any(StructuredRecord.class));
    ArgumentCaptor<StructuredRecord> captor = ArgumentCaptor.forClass(StructuredRecord.class);

    datastoreSource.initialize(mock(BatchRuntimeContext.class));
    datastoreSource.transform(input, emitter);

    verify(emitter).emit(captor.capture());
    StructuredRecord record = captor.getValue();

    assertEquals("string_value", record.get("string_field"));
    assertNull(record.get("missing_field"));
  }

  @Test
  public void testTransformDifferentType() throws Exception {
    when(config.isIncludeKey()).thenReturn(false);
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("field", Schema.of(Schema.Type.LONG))
    );
    when(config.getSchema()).thenReturn(schema);

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .set("field", "field_value")
      .build();
    when(input.getValue()).thenReturn(entity);

    thrown.expect(ClassCastException.class);
    thrown.expectMessage("java.lang.String cannot be cast to java.lang.Long");

    datastoreSource.initialize(mock(BatchRuntimeContext.class));
    datastoreSource.transform(input, emitter);
  }

  @Test
  public void testTransformOnlyKey() throws Exception {
    String keyField = "key";
    when(config.isIncludeKey()).thenReturn(true);
    when(config.getKeyType()).thenReturn(SourceKeyType.KEY_LITERAL);
    when(config.getKeyAlias()).thenReturn(keyField);
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of(keyField, Schema.of(Schema.Type.STRING))
    );
    when(config.getSchema()).thenReturn(schema);

    Entity entity = Entity.newBuilder(Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT,
                                                     DatastoreSourceConfigHelper.TEST_KIND, 1).build())
      .build();
    when(input.getValue()).thenReturn(entity);

    doNothing().when(emitter).emit(any(StructuredRecord.class));
    ArgumentCaptor<StructuredRecord> captor = ArgumentCaptor.forClass(StructuredRecord.class);

    datastoreSource.initialize(mock(BatchRuntimeContext.class));
    datastoreSource.transform(input, emitter);

    verify(emitter).emit(captor.capture());
    StructuredRecord record = captor.getValue();

    assertEquals(String.format("key(%s, %s)", DatastoreSourceConfigHelper.TEST_KIND, 1), record.get(keyField));
  }

}
