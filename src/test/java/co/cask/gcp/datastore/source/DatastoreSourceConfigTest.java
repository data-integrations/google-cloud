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
import co.cask.gcp.datastore.source.util.SourceKeyType;
import co.cask.gcp.datastore.util.DatastorePropertyUtil;
import com.google.cloud.datastore.PathElement;
import com.google.datastore.v1.client.DatastoreHelper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

/**
 * Tests for {@link DatastoreSourceConfig}.
 */
public class DatastoreSourceConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetNamespaceNull() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setNamespace(null)
      .build();

    assertEquals(DatastorePropertyUtil.DEFAULT_NAMESPACE, config.getNamespace());
  }

  @Test
  public void testGetNamespaceEmpty() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setNamespace("")
      .build();

    assertEquals(DatastorePropertyUtil.DEFAULT_NAMESPACE, config.getNamespace());
  }

  @Test
  public void testGetNamespaceNotEmpty() {
    String namespace = DatastoreSourceConfigHelper.TEST_NAMESPACE;
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setNamespace(namespace)
      .build();

    assertEquals(namespace, config.getNamespace());
  }

  @Test
  public void testGetAncestorNull() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setAncestor(null)
      .build();

    assertEquals(Collections.emptyList(), config.getAncestor());
  }

  @Test
  public void testGetAncestorEmpty() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setAncestor("")
      .build();

    assertEquals(Collections.emptyList(), config.getAncestor());
  }

  @Test
  public void testGetAncestorNotEmpty() {
    String ancestor = "Key(A,100,B,'bId',`C C C`, 123)";
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setAncestor(ancestor)
      .build();

    List<PathElement> expectedAncestor = Arrays.asList(PathElement.of("A", 100),
                                                       PathElement.of("B", "bId"),
                                                       PathElement.of("C C C", 123));

    assertEquals(expectedAncestor, config.getAncestor());
  }

  @Test
  public void testGetFiltersNull() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setFilters(null)
      .build();

    assertEquals(Collections.emptyMap(), config.getFilters());
  }

  @Test
  public void testGetFiltersEmpty() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setFilters("")
      .build();

    assertEquals(Collections.emptyMap(), config.getFilters());
  }

  @Test
  public void testGetFiltersNotEmpty() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setFilters("f1|v1;f2|10;f3|null")
      .build();

    Map<String, String> filters = new LinkedHashMap<>();
    filters.put("f1", "v1");
    filters.put("f2", "10");
    filters.put("f3", "null");

    assertEquals(filters, config.getFilters());
  }

  @Test
  public void testGetKeyTypeNone() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.NONE.getValue())
      .build();

    assertEquals(SourceKeyType.NONE, config.getKeyType());
  }

  @Test
  public void testGetKeyTypeKeyLiteral() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .build();

    assertEquals(SourceKeyType.KEY_LITERAL, config.getKeyType());
  }

  @Test
  public void testGetKeyTypeUrlSafeKey() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.URL_SAFE_KEY.getValue())
      .build();

    assertEquals(SourceKeyType.URL_SAFE_KEY, config.getKeyType());
  }

  @Test
  public void testGetKeyTypeInvalid() {
    String keyType = "invalid";
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(keyType)
      .build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported key type value: " + keyType);

    config.getKeyType();
  }

  @Test
  public void testIsIncludeKeyNone() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.NONE.getValue())
      .build();

    assertFalse(config.isIncludeKey());
  }

  @Test
  public void testIsIncludeKeyLeyLiteral() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .build();

    assertTrue(config.isIncludeKey());
  }

  @Test
  public void testIsIncludeKeyUrlSafeKey() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.URL_SAFE_KEY.getValue())
      .build();

    assertTrue(config.isIncludeKey());
  }

  @Test
  public void testValidateKindNull() {
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setKind(null)
      .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Kind can not be null or empty");

    config.validate();
  }

  @Test
  public void testValidateKindEmpty() {
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setKind("")
      .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Kind can not be null or empty");

    config.validate();
  }

  @Test
  public void testValidateKindNotEmpty() {
    Schema schema = Schema.recordOf("record", Schema.Field.of("id", Schema.of(Schema.Type.LONG)));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKind(DatastoreSourceConfigHelper.TEST_KIND)
      .build());

    config.validate();
  }

  @Test
  public void testValidateAncestorNull() {
    Schema schema = Schema.recordOf("record", Schema.Field.of("id", Schema.of(Schema.Type.LONG)));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setAncestor(null)
      .build());

    config.validate();
  }

  @Test
  public void testValidateAncestorEmpty() {
    Schema schema = Schema.recordOf("record", Schema.Field.of("id", Schema.of(Schema.Type.LONG)));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setAncestor("")
      .build());

    config.validate();
  }

  @Test
  public void testValidateAncestorNotEmpty() {
    Schema schema = Schema.recordOf("record", Schema.Field.of("id", Schema.of(Schema.Type.LONG)));
    String ancestor = "Key(A,100,B,'bId',`C C C`, 123)";

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setAncestor(ancestor)
      .build());

    config.validate();
  }

  @Test
  public void testValidateConfigNumSplitsInvalid() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(0)
      .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Number of split must be greater than 0");

    config.validate();
  }

  @Test
  public void testValidateConfigNumSplitsValid() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build());

    config.validate();
  }

  @Test
  public void testValidateConfigSchemaValid() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build());

    config.validate();
  }

  @Test
  public void testValidateConfigSchemaInvalidType() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported type: " + Schema.Type.INT);

    config.validate();
  }

  @Test
  public void testValidateConfigSchemaInvalidJson() {
    String schema = "{type: int}";
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema)
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unable to parse output schema: " + schema);

    config.validate();
  }

  @Test
  public void testValidateConfigFiltersValid() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .setFilters("name|abc")
      .build());

    config.validate();
  }

  @Test
  public void testValidateConfigFiltersInvalidString() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .setFilters("name:abc;id:1")
      .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid syntax for key-value pair in list");

    config.validate();
  }

  @Test
  public void testValidateConfigFilterMissingInSchema() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .setFilters("name|abc;type|none")
      .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("The following properties are missing in the schema definition: [type]");

    config.validate();
  }

  @Test
  public void testValidateConfigValidNoneKey() {
    Schema schema = Schema.recordOf("record",
       Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
       Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build());

    config.validate();
  }

  @Test
  public void testValidateConfigValidKeyLiteral() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setNumSplits(1)
      .build());

    config.validate();
  }

  @Test
  public void testValidateConfigValidUrlSafeKey() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.URL_SAFE_KEY.getValue())
      .setNumSplits(1)
      .build());

    config.validate();
  }

  @Test
  public void testValidateConfigKeyMissingInSchema() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setNumSplits(1)
      .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Key field [%s] must exist in the schema", DatastoreHelper.KEY_PROPERTY_NAME));

    config.validate();
  }

  @Test
  public void testValidateConfigKeyInvalidType() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.of(Schema.Type.LONG))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setNumSplits(1)
      .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Key field [%s] type must be non-nullable STRING, not %s",
      DatastoreHelper.KEY_PROPERTY_NAME, Schema.Type.LONG));

    config.validate();
  }

  @Test
  public void testValidateConfigKeyNullable() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setNumSplits(1)
      .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Key field [%s] type must be non-nullable STRING, not nullable %s",
      DatastoreHelper.KEY_PROPERTY_NAME, Schema.Type.STRING));

    config.validate();
  }

  @Test
  public void testValidateConfigValidKeyAlias() {
    String keyAlias = "key_alias";
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(keyAlias, Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setKeyAlias(keyAlias)
      .setNumSplits(1)
      .build());

    config.validate();
  }

  @Test
  public void testValidateConfigAliasSetWhenIncludeKeyFalse() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING))
    );

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setKeyAlias("key_alias")
      .setNumSplits(1)
      .build());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Key alias must be empty if Include Key is set to [%s]",
      SourceKeyType.NONE.getValue()));

    config.validate();
  }

  private DatastoreSourceConfig withDatastoreValidationMock(DatastoreSourceConfig config) {
    DatastoreSourceConfig spy = spy(config);
    doNothing().when(spy).validateDatastoreConnection();
    return spy;
  }

}
