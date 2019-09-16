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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
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

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(Collections.emptyList(), config.getAncestor(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGetAncestorEmpty() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setAncestor("")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(Collections.emptyList(), config.getAncestor(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
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

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(expectedAncestor, config.getAncestor(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
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

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(SourceKeyType.NONE, config.getKeyType(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGetKeyTypeKeyLiteral() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(SourceKeyType.KEY_LITERAL, config.getKeyType(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGetKeyTypeUrlSafeKey() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.URL_SAFE_KEY.getValue())
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(SourceKeyType.URL_SAFE_KEY, config.getKeyType(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGetKeyTypeInvalid() {
    String keyType = "invalid";
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(keyType)
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    try {
      config.getKeyType(collector);
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KEY_TYPE, e.getFailures().get(0)
        .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testIsIncludeKeyNone() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.NONE.getValue())
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertFalse(config.isIncludeKey(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testIsIncludeKeyLeyLiteral() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertTrue(config.isIncludeKey(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testIsIncludeKeyUrlSafeKey() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setKeyType(SourceKeyType.URL_SAFE_KEY.getValue())
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertTrue(config.isIncludeKey(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateKindNull() {
    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setKind(null)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KIND, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateKindEmpty() {
    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setKind("")
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KIND, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateKindNotEmpty() {
    Schema schema = Schema.recordOf("record", Schema.Field.of("id", Schema.of(Schema.Type.LONG)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKind(DatastoreSourceConfigHelper.TEST_KIND)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateAncestorNull() {
    Schema schema = Schema.recordOf("record", Schema.Field.of("id", Schema.of(Schema.Type.LONG)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setAncestor(null)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateAncestorEmpty() {
    Schema schema = Schema.recordOf("record", Schema.Field.of("id", Schema.of(Schema.Type.LONG)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setAncestor("")
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateAncestorNotEmpty() {
    Schema schema = Schema.recordOf("record", Schema.Field.of("id", Schema.of(Schema.Type.LONG)));
    String ancestor = "Key(A,100,B,'bId',`C C C`, 123)";

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setAncestor(ancestor)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateConfigNumSplitsInvalid() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(0)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSourceConstants.PROPERTY_NUM_SPLITS, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateConfigNumSplitsValid() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
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
    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateConfigSchemaInvalidType() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
                                                                 .setSchema(schema.toString())
                                                                 .setKeyType(SourceKeyType.NONE.getValue())
                                                                 .setNumSplits(1)
                                                                 .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("id", collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.OUTPUT_SCHEMA_FIELD));
  }

  @Test
  public void testValidateConfigSchemaInvalidJson() {
    String schema = "{type: int}";
    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
                                                                 .setSchema(schema)
                                                                 .setKeyType(SourceKeyType.NONE.getValue())
                                                                 .setNumSplits(1)
                                                                 .build(), collector);

    try {
      config.validate(collector);
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(DatastoreSourceConstants.PROPERTY_SCHEMA, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateConfigFiltersValid() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .setFilters("name|abc")
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateConfigFiltersInvalidString() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
                                                                 .setSchema(schema.toString())
                                                                 .setKeyType(SourceKeyType.NONE.getValue())
                                                                 .setNumSplits(1)
                                                                 .setFilters("name:abc;id:1")
                                                                 .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSourceConstants.PROPERTY_FILTERS, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateConfigFilterMissingInSchema() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .setFilters("name|abc;type|none")
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSourceConstants.PROPERTY_FILTERS, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateConfigValidNoneKey() {
    Schema schema = Schema.recordOf("record",
       Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
       Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateConfigValidKeyLiteral() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setNumSplits(1)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateConfigValidUrlSafeKey() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.URL_SAFE_KEY.getValue())
      .setNumSplits(1)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateConfigKeyMissingInSchema() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setNumSplits(1)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KEY_ALIAS, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateConfigKeyInvalidType() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.of(Schema.Type.LONG)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
                                                                 .setSchema(schema.toString())
                                                                 .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
                                                                 .setNumSplits(1)
                                                                 .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KEY_ALIAS, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateConfigKeyNullable() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DatastoreHelper.KEY_PROPERTY_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
      .setNumSplits(1)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSourceConstants.PROPERTY_KEY_ALIAS, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateConfigValidKeyAlias() {
    String keyAlias = "key_alias";
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of(keyAlias, Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
                                                                 .setSchema(schema.toString())
                                                                 .setKeyType(SourceKeyType.KEY_LITERAL.getValue())
                                                                 .setKeyAlias(keyAlias)
                                                                 .setNumSplits(1)
                                                                 .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
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

    MockFailureCollector collector = new MockFailureCollector();
    Query pbQuery = config.constructPbQuery(collector);
    Assert.assertEquals(expectedPbQuery, pbQuery);
    Assert.assertEquals(0, collector.getValidationFailures().size());
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

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateConfigArrayOfArraySchema() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("array_of_array",
       Schema.nullableOf(Schema.arrayOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))))));

    MockFailureCollector collector = new MockFailureCollector();
    DatastoreSourceConfig config = withDatastoreValidationMock(DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setKeyType(SourceKeyType.NONE.getValue())
      .setNumSplits(1)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  private DatastoreSourceConfig withDatastoreValidationMock(DatastoreSourceConfig config, FailureCollector collector) {
    DatastoreSourceConfig spy = Mockito.spy(config);
    Mockito.doNothing().when(spy).validateDatastoreConnection(collector);
    return spy;
  }
}
