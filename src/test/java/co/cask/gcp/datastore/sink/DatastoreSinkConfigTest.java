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
package co.cask.gcp.datastore.sink;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.datastore.sink.util.DatastoreSinkConstants;
import co.cask.gcp.datastore.sink.util.IndexStrategy;
import co.cask.gcp.datastore.sink.util.SinkKeyType;
import co.cask.gcp.datastore.util.DatastorePropertyUtil;
import com.google.cloud.datastore.PathElement;
import com.google.datastore.v1.client.DatastoreHelper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

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

    assertEquals(DatastorePropertyUtil.DEFAULT_NAMESPACE, config.getNamespace());
  }

  @Test
  public void testGetNamespaceEmpty() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setNamespace("")
      .build();

    assertEquals(DatastorePropertyUtil.DEFAULT_NAMESPACE, config.getNamespace());
  }

  @Test
  public void testGetNamespaceNotEmpty() {
    String namespace = DatastoreSinkConfigHelper.TEST_NAMESPACE;
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setNamespace(namespace)
      .build();

    assertEquals(namespace, config.getNamespace());
  }

  @Test
  public void testGetIndexedPropertiesNull() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setIndexedProperties(null)
      .build();

    assertEquals(Collections.emptySet(), config.getIndexedProperties());
  }

  @Test
  public void testGetIndexedPropertiesEmpty() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setIndexedProperties("")
      .build();

    assertEquals(Collections.emptySet(), config.getIndexedProperties());
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
    assertNotNull(actualProps);

    assertEquals(expectedProps, new TreeSet<>(actualProps));
  }

  @Test
  public void testGetKeyTypeUnknown() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(null)
      .build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported key type value: " + null);

    config.getKeyType();
  }

  @Test
  public void testGetKeyType() {
    SinkKeyType keyType = SinkKeyType.KEY_LITERAL;
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(keyType.getValue())
      .build();

    assertEquals(keyType, config.getKeyType());
  }

  @Test
  public void testGetIndexStrategyUnknown() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setIndexStrategy(null)
      .build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported index strategy value: " + null);

    config.getIndexStrategy();
  }

  @Test
  public void testGetIndexStrategy() {
    IndexStrategy indexStrategy = IndexStrategy.NONE;
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setIndexStrategy(indexStrategy.getValue())
      .build();

    assertEquals(indexStrategy, config.getIndexStrategy());
  }

  @Test
  public void testGetKeyAliasNull() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyAlias(null)
      .build();

    assertEquals(DatastoreHelper.KEY_PROPERTY_NAME, config.getKeyAlias());
  }

  @Test
  public void testGetKeyAliasNotEmpty() {
    String keyAlias = "testKeyAlias";
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyAlias(keyAlias)
      .build();

    assertEquals(keyAlias, config.getKeyAlias());
  }

  @Test
  public void testGetAncestorNull() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setAncestor(null)
      .build();

    assertEquals(Collections.emptyList(), config.getAncestor());
  }

  @Test
  public void testGetAncestorEmpty() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setAncestor("")
      .build();

    assertEquals(Collections.emptyList(), config.getAncestor());
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

    assertEquals(expectedAncestor, config.getAncestor());
  }

  @Test
  public void testIsUseAutoGeneratedKeyFalse() {
    SinkKeyType keyType = SinkKeyType.KEY_LITERAL;
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(keyType.getValue())
      .build();

    assertFalse(config.isUseAutoGeneratedKey());
  }

  @Test
  public void testIsUseAutoGeneratedKeyTrue() {
    DatastoreSinkConfig config = DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .build();

    assertTrue(config.isUseAutoGeneratedKey());
  }

  @Test
  public void testValidateCustomKeyWithKeyAliasInSchema() {
    String keyAlias = "testKeyAlias";
    DatastoreSinkConfig config = spy(DatastoreSinkConfigHelper.newConfigBuilder()
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
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG))
    );
    doNothing().when(config).validateDatastoreConnection();
    config.validate(schema);
  }

  @Test
  public void testValidateAutoGeneratedKeySchema() {
    DatastoreSinkConfig config = spy(DatastoreSinkConfigHelper.newConfigBuilder()
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
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG))
    );
    doNothing().when(config).validateDatastoreConnection();
    config.validate(schema);
  }

  @Test
  public void testValidateIndexStrategyCustom() {
    DatastoreSinkConfig config = spy(DatastoreSinkConfigHelper.newConfigBuilder()
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
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG))
    );
    doNothing().when(config).validateDatastoreConnection();
    config.validate(schema);
  }

  @Test
  public void testValidateIndexStrategyCustomWithoutFieldNameInSchema() {
    DatastoreSinkConfig config = spy(DatastoreSinkConfigHelper.newConfigBuilder()
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
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG))
    );

    doNothing().when(config).validateDatastoreConnection();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Indexed properties should exist in schema. Missed properties: [testNameError]");

    config.validate(schema);
  }

  @Test
  public void testValidateIndexStrategyCustomWithEmptyIndexedProperties() {
    DatastoreSinkConfig config = spy(DatastoreSinkConfigHelper.newConfigBuilder()
                                       .setNamespace("testNs")
                                       .setKind("testKind")
                                       .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
                                       .setServiceFilePath(null)
                                       .setAncestor(null)
                                       .setIndexedProperties(null)
                                       .setIndexStrategy(IndexStrategy.CUSTOM.getValue())
                                       .setBatchSize(1)
                                       .build());
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG))
    );

    doNothing().when(config).validateDatastoreConnection();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Indexed properties can not be empty if index strategy is [Custom]");

    config.validate(schema);
  }

  @Test
  public void testValidateKeyLiteralTypeWithoutKeyNameInSchema() {
    DatastoreSinkConfig config = spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setNamespace("testNs")
      .setKind("testKind")
      .setKeyType(SinkKeyType.KEY_LITERAL.getValue())
      .setServiceFilePath(null)
      .setAncestor(null)
      .setIndexedProperties(null)
      .setBatchSize(1)
      .build());
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG))
    );

    doNothing().when(config).validateDatastoreConnection();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Key Alias [__key__] should exist in schema.");

    config.validate(schema);
  }

  @Test
  public void testValidateBatchSizeWithinLimit() {
    DatastoreSinkConfig config = spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(10)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG))
    );
    doNothing().when(config).validateDatastoreConnection();
    config.validate(schema);
  }

  @Test
  public void testValidateBatchSizeZero() {
    int batchSize = 0;
    DatastoreSinkConfig config = spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(batchSize)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.LONG))
    );

    doNothing().when(config).validateDatastoreConnection();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Invalid Datastore batch size: [%s]. "
      + "Minimum number of entries in one batch: [1], maximum: [%s]", batchSize,
                                       DatastoreSinkConstants.MAX_BATCH_SIZE));

    config.validate(schema);
  }

  @Test
  public void testValidateBatchNegative() {
    int batchSize = -10;
    DatastoreSinkConfig config = spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(batchSize)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.LONG))
    );

    doNothing().when(config).validateDatastoreConnection();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Invalid Datastore batch size: [%s]. "
      + "Minimum number of entries in one batch: [1], maximum: [%s]", batchSize,
                                       DatastoreSinkConstants.MAX_BATCH_SIZE));

    config.validate(schema);
  }

  @Test
  public void testValidateBatchMax() {
    DatastoreSinkConfig config = spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(DatastoreSinkConstants.MAX_BATCH_SIZE)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.LONG))
    );
    doNothing().when(config).validateDatastoreConnection();
    config.validate(schema);
  }

  @Test
  public void testValidateBatchOverMax() {
    int batchSize = DatastoreSinkConstants.MAX_BATCH_SIZE + 1;
    DatastoreSinkConfig config = spy(DatastoreSinkConfigHelper.newConfigBuilder()
      .setKeyType(SinkKeyType.AUTO_GENERATED_KEY.getValue())
      .setServiceFilePath(null)
      .setBatchSize(batchSize)
      .setIndexStrategy(IndexStrategy.ALL.getValue())
      .build());

    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("testName", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.LONG))
    );

    doNothing().when(config).validateDatastoreConnection();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Invalid Datastore batch size: [%s]. "
      + "Minimum number of entries in one batch: [1], maximum: [%s]", batchSize,
                                       DatastoreSinkConstants.MAX_BATCH_SIZE));

    config.validate(schema);
  }

}
