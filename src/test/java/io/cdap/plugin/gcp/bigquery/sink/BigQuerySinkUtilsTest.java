/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.sink;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test for {@link BigQuerySinkUtils}
 */
public class BigQuerySinkUtilsTest {

  @Test
  public void testConfigureNullBucket() {
    Configuration configuration = new Configuration();

    BigQuerySinkUtils.configureBucket(configuration, null, "some-run-id");

    Assert.assertTrue(configuration.getBoolean("fs.gs.bucket.delete.enable", false));
    Assert.assertEquals("gs://some-run-id/some-run-id", configuration.get("fs.default.name"));
    Assert.assertTrue(configuration.getBoolean("fs.gs.impl.disable.cache", false));
    Assert.assertFalse(configuration.getBoolean("fs.gs.metadata.cache.enable", true));
  }

  @Test
  public void testConfigureBucket() {
    Configuration configuration = new Configuration();

    BigQuerySinkUtils.configureBucket(configuration, "some-bucket", "some-run-id");

    Assert.assertFalse(configuration.getBoolean("fs.gs.bucket.delete.enable", false));
    Assert.assertEquals("gs://some-bucket/some-run-id", configuration.get("fs.default.name"));
    Assert.assertTrue(configuration.getBoolean("fs.gs.impl.disable.cache", false));
    Assert.assertFalse(configuration.getBoolean("fs.gs.metadata.cache.enable", true));
  }

  public void testGenerateTableFieldSchema() {
    String fieldName = "arrayOfRecords";
    Schema schema = Schema.recordOf("record",
            Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
            Schema.Field.of(fieldName,
                    Schema.arrayOf(
                            Schema.recordOf("innerRecord",
                                    Schema.Field.of("field1", Schema.of(Schema.Type.STRING))
                            )
                    )
            )
    );

    BigQueryTableFieldSchema bqTableFieldSchema =
            BigQuerySinkUtils.generateTableFieldSchema(schema.getField(fieldName));

    List<BigQueryTableFieldSchema> fields = bqTableFieldSchema.getFields();
    Assert.assertEquals(fields.size(), 1);

    BigQueryTableFieldSchema expectedSchema = new BigQueryTableFieldSchema();
    expectedSchema.setType("STRING");
    expectedSchema.setMode("REQUIRED");
    expectedSchema.setName("field1");

    Assert.assertEquals(fields.get(0), expectedSchema);
  }

  @Test
  public void testGenerateTableFieldSchemaNullable() {
    String fieldName = "arrayOfRecords";
    Schema schema = Schema.recordOf("record",
            Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
            Schema.Field.of(fieldName,
                    Schema.nullableOf(Schema.arrayOf(
                            Schema.recordOf("innerRecord",
                                    Schema.Field.of("field1", Schema.of(Schema.Type.STRING))
                            )
                    ))
            )
    );

    BigQueryTableFieldSchema bqTableFieldSchema =
            BigQuerySinkUtils.generateTableFieldSchema(schema.getField(fieldName));

    List<BigQueryTableFieldSchema> fields = bqTableFieldSchema.getFields();
    Assert.assertEquals(fields.size(), 1);

    BigQueryTableFieldSchema expectedSchema = new BigQueryTableFieldSchema();
    expectedSchema.setType("STRING");
    expectedSchema.setMode("REQUIRED");
    expectedSchema.setName("field1");

    Assert.assertEquals(fields.get(0), expectedSchema);
  }

  @Test
  public void testGenerateTableFieldSchemaNullable2() {
    String fieldName = "arrayOfRecords";
    Schema schema = Schema.recordOf("record",
            Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
            Schema.Field.of(fieldName,
                    Schema.nullableOf(Schema.arrayOf(
                            Schema.recordOf("innerRecord",
                                    Schema.Field.of("field1", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
                            )
                    ))
            )
    );

    BigQueryTableFieldSchema bqTableFieldSchema =
            BigQuerySinkUtils.generateTableFieldSchema(schema.getField(fieldName));

    List<BigQueryTableFieldSchema> fields = bqTableFieldSchema.getFields();
    Assert.assertEquals(fields.size(), 1);

    BigQueryTableFieldSchema expectedSchema = new BigQueryTableFieldSchema();
    expectedSchema.setType("STRING");
    expectedSchema.setMode("NULLABLE");
    expectedSchema.setName("field1");

    Assert.assertEquals(fields.get(0), expectedSchema);
  }

  @Test
  public void testGenerateTableFieldSchemaNullable3() {
    String fieldName = "arrayOfRecords";
    Schema schema = Schema.recordOf("record",
            Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
            Schema.Field.of(fieldName,
                    Schema.nullableOf(Schema.arrayOf(
                            Schema.nullableOf(Schema.recordOf("innerRecord",
                                    Schema.Field.of("field1", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
                            ))
                    ))
            )
    );

    BigQueryTableFieldSchema bqTableFieldSchema =
            BigQuerySinkUtils.generateTableFieldSchema(schema.getField(fieldName));

    List<BigQueryTableFieldSchema> fields = bqTableFieldSchema.getFields();
    Assert.assertEquals(fields.size(), 1);

    BigQueryTableFieldSchema expectedSchema = new BigQueryTableFieldSchema();
    expectedSchema.setType("STRING");
    expectedSchema.setMode("NULLABLE");
    expectedSchema.setName("field1");

    Assert.assertEquals(fields.get(0), expectedSchema);
  }

}
