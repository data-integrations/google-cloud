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

import com.google.cloud.datastore.PathElement;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.gcp.datastore.source.util.DatastoreSourceConstants;
import io.cdap.plugin.gcp.datastore.source.util.SourceKeyType;
import io.cdap.plugin.gcp.datastore.util.DatastorePropertyUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    Assert.assertEquals(DatastorePropertyUtil.DEFAULT_NAMESPACE, config.getNamespace());
  }

  @Test
  public void testGetNamespaceEmpty() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setNamespace("")
      .build();

    Assert.assertEquals(DatastorePropertyUtil.DEFAULT_NAMESPACE, config.getNamespace());
  }

  @Test
  public void testGetNamespaceNotEmpty() {
    String namespace = DatastoreSourceConfigHelper.TEST_NAMESPACE;
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setNamespace(namespace)
      .build();

    Assert.assertEquals(namespace, config.getNamespace());
  }

  @Test
  public void testGetAncestorNull() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setAncestor(null)
      .build();

    Assert.assertEquals(Collections.emptyList(), config.getAncestor());
  }

  @Test
  public void testGetAncestorEmpty() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setAncestor("")
      .build();

    Assert.assertEquals(Collections.emptyList(), config.getAncestor());
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

    Assert.assertEquals(expectedAncestor, config.getAncestor());
  }

  @Test
  public void testGetFiltersNull() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setFilters(null)
      .build();

    Assert.assertEquals(Collections.emptyMap(), config.getFilters());
  }

  @Test
  public void testGetFiltersEmpty() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setFilters("")
      .build();

    Assert.assertEquals(Collections.emptyMap(), config.getFilters());
  }

  @Test
  public void testGetFiltersNotEmpty() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setFilters("f1|v1;f2|10;f3|")
      .build();

    Map<String, String> filters = new LinkedHashMap<>();
    filters.put("f1", "v1");
    filters.put("f2", "10");
    filters.put("f3", "");

    Assert.assertEquals(filters, config.getFilters());
  }

  @Test
  public void testGetKeyTypeNone() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.NONE.getValue())
      .build();

    Assert.assertEquals(SourceKeyType.NONE, config.getKeyType());
  }

  @Test
  public void testGetKeyTypeKeyLiteral() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .build();

    Assert.assertEquals(SourceKeyType.KEY_LITERAL, config.getKeyType());
  }

  @Test
  public void testGetKeyTypeUrlSafeKey() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.URL_SAFE_KEY.getValue())
      .build();

    Assert.assertEquals(SourceKeyType.URL_SAFE_KEY, config.getKeyType());
  }

  @Test
  public void testGetKeyTypeInvalid() {
    String keyType = "invalid";
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(keyType)
      .build();

    try {
      config.getKeyType();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KEY_TYPE, e.getProperty());
    }
  }

  @Test
  public void testIsIncludeKeyNone() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.NONE.getValue())
      .build();

    Assert.assertFalse(config.isIncludeKey());
  }

  @Test
  public void testIsIncludeKeyLeyLiteral() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .build();

    Assert.assertTrue(config.isIncludeKey());
  }

  @Test
  public void testIsIncludeKeyUrlSafeKey() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.URL_SAFE_KEY.getValue())
      .build();

    Assert.assertTrue(config.isIncludeKey());
  }

  @Test
  public void testValidateKindNull() {
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setKind(null)
      .build());

    try {
      config.validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KIND, e.getProperty());
    }
  }

  @Test
  public void testValidateKindEmpty() {
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setKind("")
      .build());

    try {
      config.validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KIND, e.getProperty());
    }
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
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(0)
      .build());

    try {
      config.validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_NUM_SPLITS, e.getProperty());
    }
  }

  @Test
  public void testValidateConfigNumSplitsValid() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build());

    config.validate();
  }

  @Test
  public void testValidateConfigSchemaValid() {
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
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build());

    try {
      config.validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_SCHEMA, e.getProperty());
    }
  }

  @Test
  public void testValidateConfigSchemaInvalidJson() {
    String schema = "{type: int}";
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema)
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build());

    try {
      config.validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_SCHEMA, e.getProperty());
    }
  }

  @Test
  public void testValidateConfigFiltersValid() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

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
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

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
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .setFilters("name|abc;type|none")
      .build());

    try {
      config.validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_FILTERS, e.getProperty());
    }
  }

  @Test
  public void testValidateConfigValidNoneKey() {
    Schema schema = Schema.recordOf("record",
       Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
       Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

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
      Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.of(Schema.Type.STRING)));

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
      Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.of(Schema.Type.STRING)));

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
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setNumSplits(1)
      .build());

    try {
      config.validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KEY_ALIAS, e.getProperty());
    }
  }

  @Test
  public void testValidateConfigKeyInvalidType() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.of(Schema.Type.LONG)));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setNumSplits(1)
      .build());

    try {
      config.validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KEY_ALIAS, e.getProperty());
    }
  }

  @Test
  public void testValidateConfigKeyNullable() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setNumSplits(1)
      .build());

    try {
      config.validate();
      Assert.fail("Invalid config should have thrown exception");
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KEY_ALIAS, e.getProperty());
    }
  }

  @Test
  public void testValidateConfigValidKeyAlias() {
    String keyAlias = "key_alias";
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(keyAlias, Schema.of(Schema.Type.STRING)));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setKeyAlias(keyAlias)
      .setNumSplits(1)
      .build());

    config.validate();
  }

  @Test
  public void testConstructAndTransformPbQuery() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(Schema.recordOf("schema",
        Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
        Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("type", Schema.nullableOf(Schema.of(Schema.Type.STRING))))
        .toString())
      .setAncestor("key(A1, 10, A2, 'N1')")
      .setFilters("id|10;name|test;type|")
      .build();

    Filter idFilter = DatastoreHelper.makeFilter("id",
       PropertyFilter.Operator.EQUAL,
       DatastoreHelper.makeValue(10)).build();
    Filter nameFilter = DatastoreHelper.makeFilter("name",
       PropertyFilter.Operator.EQUAL,
       DatastoreHelper.makeValue("test")).build();
    Filter nullFilter = DatastoreHelper.makeFilter("type", PropertyFilter.Operator.EQUAL,
       Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build()).build();

    Filter ancestorFilter = DatastoreHelper.makeAncestorFilter(DatastoreHelper.makeKey("A1", 10, "A2", "N1")
       .setPartitionId(PartitionId.newBuilder()
          .setNamespaceId(DatastoreSourceConfigHelper.TEST_NAMESPACE)
          .setProjectId(DatastoreSourceConfigHelper.TEST_PROJECT)
          .build())
       .build())
      .build();

    Query expectedPbQuery = Query.newBuilder()
      .addKind(KindExpression.newBuilder().setName(config.getKind()))
      .setFilter(DatastoreHelper.makeAndFilter(idFilter, nameFilter, nullFilter, ancestorFilter))
      .build();

    Query pbQuery = config.constructPbQuery();
    Assert.assertEquals(expectedPbQuery, pbQuery);
  }

  @Test
  public void testValidateConfigArrayAndComplexUnionSchema() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("array_simple",
       Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING))))),
      Schema.Field.of("array_complex_union",
       Schema.nullableOf(Schema.arrayOf(Schema.unionOf(Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING))))),
      Schema.Field.of("complex_union",
       Schema.unionOf(Schema.of(Schema.Type.LONG), Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.NULL))));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build());

    config.validate();
  }

  @Test
  public void testValidateConfigArrayOfArraySchema() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("array_of_array",
       Schema.nullableOf(Schema.arrayOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))))));

    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build());

    thrown.expect(IllegalArgumentException.class);

    config.validate();
  }

  private DatastoreSourceConfig withDatastoreValidationMock(DatastoreSourceConfig config) {
    DatastoreSourceConfig spy = Mockito.spy(config);
    Mockito.doNothing().when(spy).validateDatastoreConnection();
    return spy;
  }

}
