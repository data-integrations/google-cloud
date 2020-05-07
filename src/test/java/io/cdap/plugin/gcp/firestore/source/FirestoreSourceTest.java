/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.gcp.firestore.source;

import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link FirestoreSource} class.
 */
@RunWith(MockitoJUnitRunner.class)
public class FirestoreSourceTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @InjectMocks
  private FirestoreSource firestoreSource;

  @Test
  public void testGetSchemaIsIncludeDocumentIdTrue() {
    Map<String, Object> fields = new ImmutableMap.Builder<String, Object>()
      .put("string_field", "string_value")
      .build();

    QueryDocumentSnapshot entity = Mockito.mock(QueryDocumentSnapshot.class);
    Mockito.when(entity.getData()).thenReturn(fields);
    Mockito.when(entity.get("string_field")).thenReturn("string_value");

    Schema schemaWithKey = firestoreSource.constructSchema(entity, true, "key");

    List<Schema.Field> schemaWithKeyFields = schemaWithKey.getFields();
    Assert.assertNotNull(schemaWithKeyFields);
    Assert.assertEquals(2, schemaWithKeyFields.size());
    checkField("string_field", schemaWithKey, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    checkField("key", schemaWithKey, Schema.of(Schema.Type.STRING));
  }

  @Test
  public void testGetSchemaIsIncludeDocumentIdFalse() {
    Map<String, Object> fields = new ImmutableMap.Builder<String, Object>()
      .put("string_field", "string_value")
      .build();

    QueryDocumentSnapshot entity = Mockito.mock(QueryDocumentSnapshot.class);
    Mockito.when(entity.getData()).thenReturn(fields);
    Mockito.when(entity.get("string_field")).thenReturn("string_value");

    Schema schemaWithKey = firestoreSource.constructSchema(entity, false, "key");

    List<Schema.Field> schemaWithKeyFields = schemaWithKey.getFields();
    Assert.assertNotNull(schemaWithKeyFields);
    Assert.assertEquals(1, schemaWithKeyFields.size());
    checkField("string_field", schemaWithKey, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    Schema.Field field = schemaWithKey.getField("key");
    Assert.assertNull(field);
  }

  private void checkField(String name, Schema schema, Schema fieldSchema) {
    Schema.Field field = schema.getField(name);
    Assert.assertNotNull(field);
    Assert.assertEquals(field.getSchema(), fieldSchema);
  }
}
