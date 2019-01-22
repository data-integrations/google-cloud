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
package co.cask.gcp.datastore.source.util;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.datastore.source.DatastoreSourceConfig;
import co.cask.gcp.datastore.source.DatastoreSourceConfigHelper;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.StructuredQuery;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static co.cask.gcp.datastore.source.DatastoreSourceConfigHelper.TEST_KIND;
import static co.cask.gcp.datastore.source.DatastoreSourceConfigHelper.TEST_NAMESPACE;
import static co.cask.gcp.datastore.source.DatastoreSourceConfigHelper.TEST_PROJECT;
import static org.junit.Assert.assertEquals;

/**
 * Tests for query utility methods.
 */
public class DatastoreSourceQueryUtilTest {

  @Test
  public void testConstructAncestorKey() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setAncestor("key(A1, 'N1', A2, 10, A3, 'N2', A4, 20)").build();

    Key actualKey = DatastoreSourceQueryUtil.constructAncestorKey(config);

    Key expectedKey = Key.newBuilder(TEST_PROJECT, "A4", 20)
      .setNamespace(TEST_NAMESPACE)
      .addAncestor(PathElement.of("A1", "N1"))
      .addAncestor(PathElement.of("A2", 10))
      .addAncestor(PathElement.of("A3", "N2"))
      .build();

    assertEquals(actualKey, expectedKey);
  }

  @Test
  public void testTransformKeyToKeyStringKeyLiteral() {
    Key key = Key.newBuilder(TEST_PROJECT, "key", 1)
      .setNamespace(TEST_NAMESPACE)
      .addAncestor(PathElement.of("A1", 10))
      .addAncestor(PathElement.of("A2", "N1"))
      .build();

    assertEquals("key(A1, 10, A2, 'N1', key, 1)",
                 DatastoreSourceQueryUtil.transformKeyToKeyString(key, SourceKeyType.KEY_LITERAL));
  }

  @Test
  public void testTransformKeyToKeyStringUrlSafeKey() {
    Key key = Key.newBuilder(TEST_PROJECT, "key", 1)
      .setNamespace(TEST_NAMESPACE)
      .addAncestor(PathElement.of("A1", 10))
      .addAncestor(PathElement.of("A2", "N1"))
      .build();

    assertEquals(key.toUrlSafe(), DatastoreSourceQueryUtil.transformKeyToKeyString(key, SourceKeyType.URL_SAFE_KEY));
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
      .setFilters("id|10;name|test;type|null")
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
        .setPartitionId(PartitionId.newBuilder().setNamespaceId(TEST_NAMESPACE).setProjectId(TEST_PROJECT).build())
        .build())
      .build();

    Query expectedPbQuery = Query.newBuilder()
      .addKind(KindExpression.newBuilder().setName(config.getKind()))
      .setFilter(DatastoreHelper.makeAndFilter(idFilter, nameFilter, nullFilter, ancestorFilter))
      .build();

    Query pbQuery = DatastoreSourceQueryUtil.constructPbQuery(config);
    assertEquals(expectedPbQuery, pbQuery);

    com.google.cloud.datastore.Query<Entity> expectedQuery = com.google.cloud.datastore.Query.newEntityQueryBuilder()
      .setNamespace(TEST_NAMESPACE)
      .setKind(TEST_KIND)
      .setFilter(StructuredQuery.CompositeFilter.and(
        StructuredQuery.PropertyFilter.eq("id", 10),
        StructuredQuery.PropertyFilter.eq("name", "test"),
        StructuredQuery.PropertyFilter.isNull("type"),
        StructuredQuery.PropertyFilter.hasAncestor(
          Key.newBuilder(TEST_PROJECT, "A2", "N1")
            .setNamespace(TEST_NAMESPACE)
            .addAncestor(PathElement.of("A1", 10))
            .build())))
      .build();

    Configuration hadoopConf = new Configuration();
    hadoopConf.set(DatastoreSourceConstants.CONFIG_NAMESPACE, TEST_NAMESPACE);
    hadoopConf.set(DatastoreSourceConstants.CONFIG_KIND, TEST_KIND);

    assertEquals(expectedQuery, DatastoreSourceQueryUtil.transformPbQuery(pbQuery, hadoopConf));
  }

}
