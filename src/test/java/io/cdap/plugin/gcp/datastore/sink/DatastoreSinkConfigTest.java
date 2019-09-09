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

import com.google.cloud.datastore.PathElement;
import com.google.datastore.v1.client.DatastoreHelper;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.datastore.sink.util.DatastoreSinkConstants;
import io.cdap.plugin.gcp.datastore.sink.util.IndexStrategy;
import io.cdap.plugin.gcp.datastore.sink.util.SinkKeyType;
import io.cdap.plugin.gcp.datastore.util.DatastorePropertyUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tests of {@link DatastoreSinkConfig} methods.
 */
public class DatastoreSinkConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetNamespaceNull() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setNamespace(null)
      .build();

    Assert.assertEquals(DatastorePropertyUtil.DEFAULT_NAMESPACE, config.getNamespace());
  }

  @Test
  public void testGetNamespaceEmpty() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setNamespace("")
      .build();

    Assert.assertEquals(DatastorePropertyUtil.DEFAULT_NAMESPACE, config.getNamespace());
  }

  @Test
  public void testGetNamespaceNotEmpty() {
    String namespace = DatastoreSinkConfigHelper.TEST_NAMESPACE;
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setNamespace(namespace)
      .build();

    Assert.assertEquals(namespace, config.getNamespace());
  }

  @Test
  public void testGetIndexedPropertiesNull() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setIndexedProperties(null)
      .build();

    Assert.assertEquals(Collections.emptySet(), config.getIndexedProperties());
  }

  @Test
  public void testGetIndexedPropertiesEmpty() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setIndexedProperties("")
      .build();

    Assert.assertEquals(Collections.emptySet(), config.getIndexedProperties());
  }

  @Test
  public void testGetIndexedPropertiesNotEmpty() {
    String indexedProperties = "prop1,prop2,prop3";
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setIndexedProperties(indexedProperties)
      .build();

    Set<String> expectedProps = new TreeSet<>();
    expectedProps.add("prop1");
    expectedProps.add("prop2");
    expectedProps.add("prop3");

    Set<String> actualProps = config.getIndexedProperties();
    Assert.assertEquals(expectedProps, actualProps);
  }

  @Test
  public void testGetKeyTypeUnknown() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(null)
      .build();

    try {
      MockFailureCollector collector = new MockFailureCollector();
      config.getKeyType(collector);
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(DatastoreSinkConstants.PROPERTY_KEY_TYPE, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testGetKeyType() {
    SinkKeyType keyType = SinkKeyType.KEY_LITERAL;
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(keyType.getValue())
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(keyType, config.getKeyType(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGetIndexStrategyUnknown() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setIndexStrategy(null)
      .build();

    try {
      MockFailureCollector collector = new MockFailureCollector();
      config.getIndexStrategy(collector);
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(DatastoreSinkConstants.PROPERTY_INDEX_STRATEGY, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testGetIndexStrategy() {
    IndexStrategy indexStrategy = IndexStrategy.NONE;
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setIndexStrategy(indexStrategy.getValue())
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(indexStrategy, config.getIndexStrategy(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGetKeyAliasNull() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyAlias(null)
      .build();

    Assert.assertEquals(DatastoreHelper.KEY_PROPERTY_NAME, config.getKeyAlias());
  }

  @Test
  public void testGetKeyAliasNotEmpty() {
    String keyAlias = "testKeyAlias";
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyAlias(keyAlias)
      .build();

    Assert.assertEquals(keyAlias, config.getKeyAlias());
  }

  @Test
  public void testGetAncestorNull() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setAncestor(null)
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(Collections.emptyList(), config.getAncestor(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGetAncestorEmpty() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setAncestor("")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(Collections.emptyList(), config.getAncestor(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGetAncestorNotEmpty() {
    String ancestor = "Key(A,100,B,'bId',`C C C`, 123)";
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
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
  public void testIsUseAutoGeneratedKeyFalse() {
    SinkKeyType keyType = SinkKeyType.KEY_LITERAL;
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(keyType.getValue())
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertFalse(config.shouldUseAutoGeneratedKey(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testIsUseAutoGeneratedKeyTrue() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertTrue(config.shouldUseAutoGeneratedKey(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateCustomKeyWithKeyAliasInSchema() {
    String keyAlias = "testKeyAlias";
    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setNamespace("testNs")
      .setKind("testKind")
      .setKeyType(SinkKeyType.CUSTOM_NAME.getValue())
      .setKeyAlias(keyAlias)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .setServiceFilePath(null)
      .setAncestor(null)
      .setIndexedProperties(null)
      .setBatchSize(1)
      .build());
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of(keyAlias, Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)));
    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateAutoGeneratedKeySchema() {
    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setNamespace("testNs")
      .setKind("testKind")
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setAncestor(null)
      .setIndexedProperties(null)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .setBatchSize(1)
      .build());
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)));
    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateIndexStrategyCustom() {
    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
                                       .setNamespace("testNs")
                                       .setKind("testKind")
                                       .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
                                       .setServiceFilePath(null)
                                       .setAncestor(null)
                                       .setIndexedProperties("testName")
                                       .setIndexStrategy(IndexStrategy.CUSTOM.getValue())
                                       .setBatchSize(1)
                                       .build());
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)));
    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateIndexStrategyCustomWithoutFieldNameInSchema() {
    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
                                       .setNamespace("testNs")
                                       .setKind("testKind")
                                       .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
                                       .setServiceFilePath(null)
                                       .setAncestor(null)
                                       .setIndexedProperties("testNameError")
                                       .setIndexStrategy(IndexStrategy.CUSTOM.getValue())
                                       .setBatchSize(1)
                                       .build());
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)));

    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSinkConstants.PROPERTY_INDEXED_PROPERTIES, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateKeyLiteralTypeWithoutKeyNameInSchema() {
    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setNamespace("testNs")
      .setKind("testKind")
      .setKeyType(SinkKeyType.KEY_LITERAL.getValue())
      .setServiceFilePath(null)
      .setAncestor(null)
      .setIndexedProperties(null)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .setBatchSize(1)
      .build());
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)));

    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSinkConstants.PROPERTY_KEY_ALIAS, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateBatchSizeWithinLimit() {
    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(10)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)));
    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateBatchSizeZero() {
    int batchSize = 0;
    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(batchSize)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.LONG)));
    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSinkConstants.PROPERTY_BATCH_SIZE, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateBatchNegative() {
    int batchSize = -10;
    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(batchSize)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.LONG)));

    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSinkConstants.PROPERTY_BATCH_SIZE, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateBatchMax() {
    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(DatastoreSinkConstants.MAX_BATCH_SIZE)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.LONG)));
    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateBatchOverMax() {
    int batchSize = DatastoreSinkConstants.MAX_BATCH_SIZE + 1;
    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(batchSize)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.LONG)));

    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSinkConstants.PROPERTY_BATCH_SIZE, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
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

    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(25)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateConfigArrayOfArraySchema() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("array_of_array",
       Schema.nullableOf(Schema.arrayOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))))));

    DatastoreSinkConfig config = Mockito.spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(25)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    MockFailureCollector collector = new MockFailureCollector();
    Mockito.doNothing().when(config).validateDatastoreConnection(collector);
    config.validate(schema, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("array_of_array", collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.INPUT_SCHEMA_FIELD));
  }
}
