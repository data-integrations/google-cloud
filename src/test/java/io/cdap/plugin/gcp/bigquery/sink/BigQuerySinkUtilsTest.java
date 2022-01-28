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

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize;
import org.apache.hadoop.conf.Configuration;
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

  @Test
  public void testNumericPrecision() {
    List<BigQueryTableFieldSchema> bqSchema;

    // Maximum Numeric precision and scale.
    bqSchema = BigQuerySinkUtils.getBigQueryTableFieldsFromSchema(
      Schema.recordOf("testName", Schema.Field.of(
        "testField", Schema.decimalOf(BigQueryTypeSize.Numeric.PRECISION, BigQueryTypeSize.Numeric.SCALE)
      )));
    Assert.assertEquals(bqSchema.get(0).getType(), LegacySQLTypeName.NUMERIC.toString());

    // Precision higher than Numeric
    bqSchema = BigQuerySinkUtils.getBigQueryTableFieldsFromSchema(
      Schema.recordOf("testName", Schema.Field.of(
        "testField", Schema.decimalOf(BigQueryTypeSize.Numeric.PRECISION + 1,
                                      BigQueryTypeSize.Numeric.SCALE)
      )));
    Assert.assertEquals(bqSchema.get(0).getType(), LegacySQLTypeName.BIGNUMERIC.toString());

    // Scale higher than Numeric
    bqSchema = BigQuerySinkUtils.getBigQueryTableFieldsFromSchema(
      Schema.recordOf("testName", Schema.Field.of(
        "testField", Schema.decimalOf(BigQueryTypeSize.Numeric.PRECISION,
                                      BigQueryTypeSize.Numeric.SCALE + 1)
      )));
    Assert.assertEquals(bqSchema.get(0).getType(), LegacySQLTypeName.BIGNUMERIC.toString());

    // Precision and Scale higher than Numeric
    bqSchema = BigQuerySinkUtils.getBigQueryTableFieldsFromSchema(
      Schema.recordOf("testName", Schema.Field.of(
        "testField", Schema.decimalOf(BigQueryTypeSize.Numeric.PRECISION + 1,
                                      BigQueryTypeSize.Numeric.SCALE + 1)
      )));
    Assert.assertEquals(bqSchema.get(0).getType(), LegacySQLTypeName.BIGNUMERIC.toString());

    // Difference between Precision and Scale larger than Numeric max difference.
    bqSchema = BigQuerySinkUtils.getBigQueryTableFieldsFromSchema(
      Schema.recordOf("testName", Schema.Field.of(
        "testField", Schema.decimalOf(BigQueryTypeSize.Numeric.PRECISION,
                                      BigQueryTypeSize.Numeric.SCALE - 1)
      )));
    Assert.assertEquals(bqSchema.get(0).getType(), LegacySQLTypeName.BIGNUMERIC.toString());
  }


}
