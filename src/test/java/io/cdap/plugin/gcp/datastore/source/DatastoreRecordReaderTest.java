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

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.StructuredQuery;
import com.google.datastore.v1.Query;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.datastore.source.util.DatastoreSourceConstants;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link DatastoreRecordReader}.
 */
public class DatastoreRecordReaderTest {

  @Test
  public void testTransformPbQuery() {
    DatastoreSourceConfig config = DatastoreSourceConfigHelper.newConfigBuilder()
      .setSchema(Schema.recordOf("schema",
                                 Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                 Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                 Schema.Field.of("type", Schema.nullableOf(Schema.of(Schema.Type.STRING))))
                   .toString())
      .setAncestor("key(A1, 10, A2, 'N1')")
      .setFilters("id|10;name|test;type|")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Query pbQuery = config.constructPbQuery(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());

    com.google.cloud.datastore.Query<Entity> expectedQuery = com.google.cloud.datastore.Query.newEntityQueryBuilder()
      .setNamespace(DatastoreSourceConfigHelper.TEST_NAMESPACE)
      .setKind(DatastoreSourceConfigHelper.TEST_KIND)
      .setFilter(StructuredQuery.CompositeFilter.and(
        StructuredQuery.PropertyFilter.eq("id", 10),
        StructuredQuery.PropertyFilter.eq("name", "test"),
        StructuredQuery.PropertyFilter.isNull("type"),
        StructuredQuery.PropertyFilter.hasAncestor(
          Key.newBuilder(DatastoreSourceConfigHelper.TEST_PROJECT, "A2", "N1")
            .setNamespace(DatastoreSourceConfigHelper.TEST_NAMESPACE)
            .addAncestor(PathElement.of("A1", 10))
            .build())))
      .build();

    Configuration hadoopConf = new Configuration();
    hadoopConf.set(DatastoreSourceConstants.CONFIG_NAMESPACE, DatastoreSourceConfigHelper.TEST_NAMESPACE);
    hadoopConf.set(DatastoreSourceConstants.CONFIG_KIND, DatastoreSourceConfigHelper.TEST_KIND);

    Assert.assertEquals(expectedQuery, new DatastoreRecordReader().transformPbQuery(pbQuery, hadoopConf));
  }

}
