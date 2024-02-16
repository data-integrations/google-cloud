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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableId;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.sink.lib.BigQueryTableFieldSchema;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
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

  @Test
  public void testCDAPtoBQSchemaMappingForLogicalTypes() {
    Schema cdapSchema;
    com.google.cloud.bigquery.Schema bqSchema;

    cdapSchema = Schema.recordOf(
      "testName",
      Schema.Field.of("dateField", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("timeMillisField", Schema.of(Schema.LogicalType.TIME_MILLIS)),
      Schema.Field.of("timeMicrosField", Schema.of(Schema.LogicalType.TIME_MICROS)),
      Schema.Field.of("tsMillisField", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
      Schema.Field.of("tsMicrosField", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("decimalSmallField",
                      Schema.decimalOf(BigQueryTypeSize.Numeric.PRECISION,
                                       BigQueryTypeSize.Numeric.SCALE)),
      Schema.Field.of("decimalLargeField",
                      Schema.decimalOf(BigQueryTypeSize.Numeric.PRECISION + 1,
                                       BigQueryTypeSize.Numeric.SCALE + 1)),
      Schema.Field.of("datetimeField",
                      Schema.of(Schema.LogicalType.DATETIME))
    );
    bqSchema = BigQuerySinkUtils.convertCdapSchemaToBigQuerySchema(cdapSchema);

    // Check date field
    Assert.assertEquals(bqSchema.getFields().get(0).getName(), "dateField");
    Assert.assertEquals(bqSchema.getFields().get(0).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(0).getType(), LegacySQLTypeName.DATE);

    // Check time fields
    Assert.assertEquals(bqSchema.getFields().get(1).getName(), "timeMillisField");
    Assert.assertEquals(bqSchema.getFields().get(1).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(1).getType(), LegacySQLTypeName.TIME);
    Assert.assertEquals(bqSchema.getFields().get(2).getName(), "timeMicrosField");
    Assert.assertEquals(bqSchema.getFields().get(2).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(2).getType(), LegacySQLTypeName.TIME);

    // Check Timestamp Fields
    Assert.assertEquals(bqSchema.getFields().get(3).getName(), "tsMillisField");
    Assert.assertEquals(bqSchema.getFields().get(3).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(3).getType(), LegacySQLTypeName.TIMESTAMP);
    Assert.assertEquals(bqSchema.getFields().get(4).getName(), "tsMicrosField");
    Assert.assertEquals(bqSchema.getFields().get(4).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(4).getType(), LegacySQLTypeName.TIMESTAMP);

    // Check Decimal Fields
    Assert.assertEquals(bqSchema.getFields().get(5).getName(), "decimalSmallField");
    Assert.assertEquals(bqSchema.getFields().get(5).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(5).getType(), LegacySQLTypeName.NUMERIC);
    Assert.assertEquals(bqSchema.getFields().get(5).getPrecision(),
                        Long.valueOf(BigQueryTypeSize.Numeric.PRECISION));
    Assert.assertEquals(bqSchema.getFields().get(5).getScale(),
                        Long.valueOf(BigQueryTypeSize.Numeric.SCALE));
    Assert.assertEquals(bqSchema.getFields().get(6).getName(), "decimalLargeField");
    Assert.assertEquals(bqSchema.getFields().get(6).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(6).getType(), LegacySQLTypeName.BIGNUMERIC);
    Assert.assertEquals(bqSchema.getFields().get(6).getPrecision(),
                        Long.valueOf(BigQueryTypeSize.Numeric.PRECISION + 1));
    Assert.assertEquals(bqSchema.getFields().get(6).getScale(),
                        Long.valueOf(BigQueryTypeSize.Numeric.SCALE + 1));

    // Check datetime field
    Assert.assertEquals(bqSchema.getFields().get(7).getName(), "datetimeField");
    Assert.assertEquals(bqSchema.getFields().get(7).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(7).getType(), LegacySQLTypeName.DATETIME);
  }

  @Test
  public void testCDAPtoBQSchemaMappingForNullableLogicalTypes() {
    Schema cdapSchema;
    com.google.cloud.bigquery.Schema bqSchema;

    cdapSchema = Schema.recordOf(
      "testName",
      Schema.Field.of("dateField", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
      Schema.Field.of("timeMillisField", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MILLIS))),
      Schema.Field.of("timeMicrosField", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS))),
      Schema.Field.of("tsMillisField", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))),
      Schema.Field.of("tsMicrosField", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("decimalSmallField",
                      Schema.nullableOf(Schema.decimalOf(BigQueryTypeSize.Numeric.PRECISION,
                                                         BigQueryTypeSize.Numeric.SCALE))),
      Schema.Field.of("decimalLargeField",
                      Schema.nullableOf(Schema.decimalOf(BigQueryTypeSize.Numeric.PRECISION + 1,
                                                         BigQueryTypeSize.Numeric.SCALE + 1))),
      Schema.Field.of("datetimeField",
                      Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME)))
    );
    bqSchema = BigQuerySinkUtils.convertCdapSchemaToBigQuerySchema(cdapSchema);

    // Check date field
    Assert.assertEquals(bqSchema.getFields().get(0).getName(), "dateField");
    Assert.assertEquals(bqSchema.getFields().get(0).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(0).getType(), LegacySQLTypeName.DATE);

    // Check time fields
    Assert.assertEquals(bqSchema.getFields().get(1).getName(), "timeMillisField");
    Assert.assertEquals(bqSchema.getFields().get(1).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(1).getType(), LegacySQLTypeName.TIME);
    Assert.assertEquals(bqSchema.getFields().get(2).getName(), "timeMicrosField");
    Assert.assertEquals(bqSchema.getFields().get(2).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(2).getType(), LegacySQLTypeName.TIME);

    // Check Timestamp Fields
    Assert.assertEquals(bqSchema.getFields().get(3).getName(), "tsMillisField");
    Assert.assertEquals(bqSchema.getFields().get(3).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(3).getType(), LegacySQLTypeName.TIMESTAMP);
    Assert.assertEquals(bqSchema.getFields().get(4).getName(), "tsMicrosField");
    Assert.assertEquals(bqSchema.getFields().get(4).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(4).getType(), LegacySQLTypeName.TIMESTAMP);

    // Check Decimal Fields
    Assert.assertEquals(bqSchema.getFields().get(5).getName(), "decimalSmallField");
    Assert.assertEquals(bqSchema.getFields().get(5).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(5).getType(), LegacySQLTypeName.NUMERIC);
    Assert.assertEquals(bqSchema.getFields().get(5).getPrecision(),
                        Long.valueOf(BigQueryTypeSize.Numeric.PRECISION));
    Assert.assertEquals(bqSchema.getFields().get(5).getScale(),
                        Long.valueOf(BigQueryTypeSize.Numeric.SCALE));
    Assert.assertEquals(bqSchema.getFields().get(6).getName(), "decimalLargeField");
    Assert.assertEquals(bqSchema.getFields().get(6).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(6).getType(), LegacySQLTypeName.BIGNUMERIC);
    Assert.assertEquals(bqSchema.getFields().get(6).getPrecision(),
                        Long.valueOf(BigQueryTypeSize.Numeric.PRECISION + 1));
    Assert.assertEquals(bqSchema.getFields().get(6).getScale(),
                        Long.valueOf(BigQueryTypeSize.Numeric.SCALE + 1));

    // Check datetime field
    Assert.assertEquals(bqSchema.getFields().get(7).getName(), "datetimeField");
    Assert.assertEquals(bqSchema.getFields().get(7).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(7).getType(), LegacySQLTypeName.DATETIME);
  }

  @Test
  public void testCDAPtoBQSchemaMappingForBasicTypes() {
    Schema cdapSchema;
    com.google.cloud.bigquery.Schema bqSchema;

    Schema nestedSchema = Schema.recordOf(
      "nested",
      Schema.Field.of("nestedIntField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("nestedBooleanField", Schema.of(Schema.Type.BOOLEAN))
    );
    cdapSchema = Schema.recordOf(
      "testName",
      Schema.Field.of("intField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("longField", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("stringField", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("floatField", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("doubleField", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("booleanField", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("bytesField", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("intArrayField", Schema.arrayOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("stringArrayField", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("recordField", nestedSchema)
    );
    bqSchema = BigQuerySinkUtils.convertCdapSchemaToBigQuerySchema(cdapSchema);

    // Check int/long fields
    Assert.assertEquals(bqSchema.getFields().get(0).getName(), "intField");
    Assert.assertEquals(bqSchema.getFields().get(0).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(0).getType(), LegacySQLTypeName.INTEGER);
    Assert.assertEquals(bqSchema.getFields().get(1).getName(), "longField");
    Assert.assertEquals(bqSchema.getFields().get(1).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(1).getType(), LegacySQLTypeName.INTEGER);

    // check string field
    Assert.assertEquals(bqSchema.getFields().get(2).getName(), "stringField");
    Assert.assertEquals(bqSchema.getFields().get(2).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(2).getType(), LegacySQLTypeName.STRING);

    // check float/double fields
    Assert.assertEquals(bqSchema.getFields().get(3).getName(), "floatField");
    Assert.assertEquals(bqSchema.getFields().get(3).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(3).getType(), LegacySQLTypeName.FLOAT);
    Assert.assertEquals(bqSchema.getFields().get(4).getName(), "doubleField");
    Assert.assertEquals(bqSchema.getFields().get(4).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(4).getType(), LegacySQLTypeName.FLOAT);

    // check boolean field
    Assert.assertEquals(bqSchema.getFields().get(5).getName(), "booleanField");
    Assert.assertEquals(bqSchema.getFields().get(5).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(5).getType(), LegacySQLTypeName.BOOLEAN);

    // check bytes field
    Assert.assertEquals(bqSchema.getFields().get(6).getName(), "bytesField");
    Assert.assertEquals(bqSchema.getFields().get(6).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(6).getType(), LegacySQLTypeName.BYTES);

    // check arrayFields field
    Assert.assertEquals(bqSchema.getFields().get(7).getName(), "intArrayField");
    Assert.assertEquals(bqSchema.getFields().get(7).getMode(), Field.Mode.REPEATED);
    Assert.assertEquals(bqSchema.getFields().get(7).getType(), LegacySQLTypeName.INTEGER);
    Assert.assertEquals(bqSchema.getFields().get(8).getName(), "stringArrayField");
    Assert.assertEquals(bqSchema.getFields().get(8).getMode(), Field.Mode.REPEATED);
    Assert.assertEquals(bqSchema.getFields().get(8).getType(), LegacySQLTypeName.STRING);

    // check record field
    Assert.assertEquals(bqSchema.getFields().get(9).getName(), "recordField");
    Assert.assertEquals(bqSchema.getFields().get(9).getMode(), Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(9).getType(), LegacySQLTypeName.RECORD);
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(0).getName(),
                        "nestedIntField");
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(0).getMode(),
                        Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(0).getType(),
                        LegacySQLTypeName.INTEGER);
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(1).getName(),
                        "nestedBooleanField");
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(1).getMode(),
                        Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(1).getType(),
                        LegacySQLTypeName.BOOLEAN);
  }

  @Test
  public void testCDAPtoBQSchemaMappingForNullableBasicTypes() {
    Schema cdapSchema;
    com.google.cloud.bigquery.Schema bqSchema;

    Schema nestedSchema = Schema.recordOf(
      "nested",
      Schema.Field.of("nestedIntField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("nestedBooleanField", Schema.of(Schema.Type.BOOLEAN))
    );
    cdapSchema = Schema.recordOf(
      "testName",
      Schema.Field.of("intField", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("longField", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("stringField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("floatField", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("doubleField", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("booleanField", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("bytesField", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("intArrayField", Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.INT)))),
      Schema.Field.of("stringArrayField", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))),
      Schema.Field.of("recordField", Schema.nullableOf(nestedSchema))
    );
    bqSchema = BigQuerySinkUtils.convertCdapSchemaToBigQuerySchema(cdapSchema);

    // Check int/long fields
    Assert.assertEquals(bqSchema.getFields().get(0).getName(), "intField");
    Assert.assertEquals(bqSchema.getFields().get(0).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(0).getType(), LegacySQLTypeName.INTEGER);
    Assert.assertEquals(bqSchema.getFields().get(1).getName(), "longField");
    Assert.assertEquals(bqSchema.getFields().get(1).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(1).getType(), LegacySQLTypeName.INTEGER);

    // check string field
    Assert.assertEquals(bqSchema.getFields().get(2).getName(), "stringField");
    Assert.assertEquals(bqSchema.getFields().get(2).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(2).getType(), LegacySQLTypeName.STRING);

    // check float/double fields
    Assert.assertEquals(bqSchema.getFields().get(3).getName(), "floatField");
    Assert.assertEquals(bqSchema.getFields().get(3).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(3).getType(), LegacySQLTypeName.FLOAT);
    Assert.assertEquals(bqSchema.getFields().get(4).getName(), "doubleField");
    Assert.assertEquals(bqSchema.getFields().get(4).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(4).getType(), LegacySQLTypeName.FLOAT);

    // check boolean field
    Assert.assertEquals(bqSchema.getFields().get(5).getName(), "booleanField");
    Assert.assertEquals(bqSchema.getFields().get(5).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(5).getType(), LegacySQLTypeName.BOOLEAN);

    // check bytes field
    Assert.assertEquals(bqSchema.getFields().get(6).getName(), "bytesField");
    Assert.assertEquals(bqSchema.getFields().get(6).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(6).getType(), LegacySQLTypeName.BYTES);

    // check arrayFields field
    Assert.assertEquals(bqSchema.getFields().get(7).getName(), "intArrayField");
    Assert.assertEquals(bqSchema.getFields().get(7).getMode(), Field.Mode.REPEATED);
    Assert.assertEquals(bqSchema.getFields().get(7).getType(), LegacySQLTypeName.INTEGER);
    Assert.assertEquals(bqSchema.getFields().get(8).getName(), "stringArrayField");
    Assert.assertEquals(bqSchema.getFields().get(8).getMode(), Field.Mode.REPEATED);
    Assert.assertEquals(bqSchema.getFields().get(8).getType(), LegacySQLTypeName.STRING);

    // check record field
    Assert.assertEquals(bqSchema.getFields().get(9).getName(), "recordField");
    Assert.assertEquals(bqSchema.getFields().get(9).getMode(), Field.Mode.NULLABLE);
    Assert.assertEquals(bqSchema.getFields().get(9).getType(), LegacySQLTypeName.RECORD);
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(0).getName(),
                        "nestedIntField");
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(0).getMode(),
                        Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(0).getType(),
                        LegacySQLTypeName.INTEGER);
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(1).getName(),
                        "nestedBooleanField");
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(1).getMode(),
                        Field.Mode.REQUIRED);
    Assert.assertEquals(bqSchema.getFields().get(9).getSubFields().get(1).getType(),
                        LegacySQLTypeName.BOOLEAN);
  }

  @Test
  public void testCreateDatasetIfNotExists() throws IOException {
    String dsProjectId = "dummy_project";
    String dsName = "dummy_dataset";
    BigQuery bq = Mockito.mock(BigQuery.class);
    DatasetId dsId = DatasetId.of(dsProjectId, dsName);
    Dataset ds = Mockito.mock(Dataset.class);

    // when dataset exists do not try to create dataset
    Mockito.when(bq.getDataset(dsId)).thenReturn(ds);
    BigQuerySinkUtils.createDatasetIfNotExists(bq, dsId, null, null,
                                               () -> String.format("Unable to create BigQuery dataset '%s.%s'",
                                                                   dsProjectId, dsName));
    Mockito.verify(bq, Mockito.times(0)).create(ArgumentMatchers.any(DatasetInfo.class));

    // when dataset does not exist create dataset
    Mockito.when(bq.getDataset(dsId)).thenReturn(null);
    Mockito.when(bq.create(ArgumentMatchers.any(DatasetInfo.class))).thenReturn(ds);
    BigQuerySinkUtils.createDatasetIfNotExists(bq, dsId, null, null,
                                               () -> String.format("Unable to create BigQuery dataset '%s.%s'",
                                                                   dsProjectId, dsName));
    Mockito.verify(bq, Mockito.times(1)).create(ArgumentMatchers.any(DatasetInfo.class));
  }

  @Test
  public void testGetRelaxedTableFieldsNoOverlap() {
    List<Field> sourceFields = new ArrayList<>(2);
    sourceFields.add(Field.newBuilder("a", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());
    sourceFields.add(Field.newBuilder("b", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());

    List<Field> destinationFields = new ArrayList<>(2);
    destinationFields.add(Field.newBuilder("x", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());
    destinationFields.add(Field.newBuilder("y", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());

    List<Field> result = BigQuerySinkUtils.getRelaxedTableFields(sourceFields, destinationFields);
    Assert.assertEquals(4, result.size());
    Assert.assertEquals("x", result.get(0).getName());
    Assert.assertEquals(Field.Mode.REQUIRED, result.get(0).getMode());
    Assert.assertEquals("y", result.get(1).getName());
    Assert.assertEquals(Field.Mode.NULLABLE, result.get(1).getMode());
    Assert.assertEquals("a", result.get(2).getName());
    Assert.assertEquals(Field.Mode.REQUIRED, result.get(2).getMode());
    Assert.assertEquals("b", result.get(3).getName());
    Assert.assertEquals(Field.Mode.NULLABLE, result.get(3).getMode());
  }

  @Test
  public void testGetRelaxedTableFieldsFullOverlap() {
    List<Field> sourceFields = new ArrayList<>(2);
    sourceFields.add(Field.newBuilder("a", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());
    sourceFields.add(Field.newBuilder("b", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());

    List<Field> destinationFields = new ArrayList<>(2);
    destinationFields.add(Field.newBuilder("a", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());
    destinationFields.add(Field.newBuilder("b", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());

    List<Field> result = BigQuerySinkUtils.getRelaxedTableFields(sourceFields, destinationFields);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals("a", result.get(0).getName());
    Assert.assertEquals(Field.Mode.REQUIRED, result.get(0).getMode());
    // Ensure the second field is nullable, as the destination table has this field as nullable.
    Assert.assertEquals("b", result.get(1).getName());
    Assert.assertEquals(Field.Mode.NULLABLE, result.get(1).getMode());
  }

  @Test
  public void testGetRelaxedTableFieldsPartialOverlap() {
    List<Field> sourceFields = new ArrayList<>(4);
    sourceFields.add(Field.newBuilder("a", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());
    sourceFields.add(Field.newBuilder("b", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());
    sourceFields.add(Field.newBuilder("x", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());
    sourceFields.add(Field.newBuilder("y", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());

    List<Field> destinationFields = new ArrayList<>(4);
    destinationFields.add(Field.newBuilder("a", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());
    destinationFields.add(Field.newBuilder("b", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
    destinationFields.add(Field.newBuilder("c", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());
    destinationFields.add(Field.newBuilder("d", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());

    List<Field> result = BigQuerySinkUtils.getRelaxedTableFields(sourceFields, destinationFields);
    Assert.assertEquals(6, result.size());
    Assert.assertEquals("c", result.get(0).getName());
    Assert.assertEquals(Field.Mode.REQUIRED, result.get(0).getMode());
    Assert.assertEquals("d", result.get(1).getName());
    Assert.assertEquals(Field.Mode.NULLABLE, result.get(1).getMode());
    Assert.assertEquals("a", result.get(2).getName());
    Assert.assertEquals(Field.Mode.REQUIRED, result.get(2).getMode());
    Assert.assertEquals("b", result.get(3).getName());
    Assert.assertEquals(Field.Mode.NULLABLE, result.get(3).getMode());
    Assert.assertEquals("x", result.get(4).getName());
    Assert.assertEquals(Field.Mode.REQUIRED, result.get(4).getMode());
    Assert.assertEquals("y", result.get(5).getName());
    Assert.assertEquals(Field.Mode.NULLABLE, result.get(5).getMode());
  }

  @Test
  public void testGetRelaxedTableFieldsMarksFieldsNullable() {
    List<Field> sourceFields = new ArrayList<>(3);
    sourceFields.add(Field.newBuilder("a", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());
    sourceFields.add(Field.newBuilder("b", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build());
    sourceFields.add(Field.newBuilder("c", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build());

    List<Field> destinationFields = new ArrayList<>(3);
    destinationFields.add(Field.newBuilder("a", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
    destinationFields.add(Field.newBuilder("b", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
    destinationFields.add(Field.newBuilder("c", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());

    List<Field> result = BigQuerySinkUtils.getRelaxedTableFields(sourceFields, destinationFields);
    // Fields a and b  should be nullable
    Assert.assertEquals(3, result.size());
    Assert.assertEquals("a", result.get(0).getName());
    Assert.assertEquals(Field.Mode.NULLABLE, result.get(0).getMode());
    Assert.assertEquals("b", result.get(1).getName());
    Assert.assertEquals(Field.Mode.NULLABLE, result.get(1).getMode());
    // Field c should be repeated as repeated fields do not get converted into null
    Assert.assertEquals("c", result.get(2).getName());
    Assert.assertEquals(Field.Mode.REPEATED, result.get(2).getMode());
  }


  @Test
  public void testGenerateUpdateUpsertQueryUpdate() {
    Operation operation = Operation.UPDATE;

    TableId sourceTableId = TableId.of("dummy_src_project", "dummy_src_dataset",
                                       "dummy_src_table");
    TableId destinationTableId = TableId.of("dummy_dest_project", "dummy_dest_dataset",
                                            "dummy_dest_table");

    List<String> tableFieldsList = new ArrayList<>(3);
    tableFieldsList.add("a");
    tableFieldsList.add("b");
    tableFieldsList.add("c");

    List<String> tableKeyList = new ArrayList<>(1);
    tableKeyList.add("a");

    List<String> orderedByList = new ArrayList<>(1);
    orderedByList.add("b ASC");
    orderedByList.add("a DESC");
    orderedByList.add("c");

    String partitionFilter = null;

    String query = BigQuerySinkUtils.generateUpdateUpsertQuery(operation, sourceTableId, destinationTableId,
                                                               tableFieldsList, tableKeyList, orderedByList,
                                                               partitionFilter);

    Assert.assertEquals("UPDATE `dummy_dest_project.dummy_dest_dataset.dummy_dest_table` T SET " +
                          "T.`b` = S.`b`, T.`c` = S.`c` FROM (SELECT * FROM (SELECT row_number() OVER " +
                          "(PARTITION BY `a` ORDER BY `b` ASC, `a` DESC, `c` ) as rowid, * " +
                          "FROM `dummy_src_project.dummy_src_dataset.dummy_src_table`) " +
                          "where rowid = 1) S WHERE T.`a` = S.`a`", query);
  }

  @Test
  public void testGenerateUpdateUpsertQueryUpsert() {
    Operation operation = Operation.UPSERT;

    TableId sourceTableId = TableId.of("dummy_src_project", "dummy_src_dataset",
                                       "dummy_src_table");
    TableId destinationTableId = TableId.of("dummy_dest_project", "dummy_dest_dataset",
                                            "dummy_dest_table");

    List<String> tableFieldsList = new ArrayList<>(3);
    tableFieldsList.add("a");
    tableFieldsList.add("b");
    tableFieldsList.add("c");

    List<String> tableKeyList = new ArrayList<>(1);
    tableKeyList.add("a");
    tableKeyList.add("b");

    List<String> orderedByList = new ArrayList<>(1);
    orderedByList.add("b ASC");
    orderedByList.add("c DESC");

    String partitionFilter = null;

    String query = BigQuerySinkUtils.generateUpdateUpsertQuery(operation, sourceTableId, destinationTableId,
                                                               tableFieldsList, tableKeyList, orderedByList,
                                                               partitionFilter);

    Assert.assertEquals("MERGE `dummy_dest_project.dummy_dest_dataset.dummy_dest_table` T USING (SELECT * " +
                          "FROM (SELECT row_number() OVER (PARTITION BY `a`, `b` " +
                          "ORDER BY `b` ASC, `c` DESC) as rowid, * FROM " +
                          "`dummy_src_project.dummy_src_dataset.dummy_src_table`) " +
                          "where rowid = 1) S ON T.`a` = S.`a` AND T.`b` = S.`b` " +
                          "WHEN MATCHED THEN UPDATE SET T.`c` = S.`c` " +
                          "WHEN NOT MATCHED THEN INSERT (`a`, `b`, `c`) VALUES(`a`, `b`, `c`)", query);
  }

  @Test
  public void testGenerateUpdateUpsertQueryUpsertWithAllFieldsAsKeys() {
    Operation operation = Operation.UPSERT;

    TableId sourceTableId = TableId.of("dummy_src_project", "dummy_src_dataset",
                                       "dummy_src_table");
    TableId destinationTableId = TableId.of("dummy_dest_project", "dummy_dest_dataset",
                                            "dummy_dest_table");

    List<String> tableFieldsList = new ArrayList<>(3);
    tableFieldsList.add("a");
    tableFieldsList.add("b");
    tableFieldsList.add("c");

    List<String> tableKeyList = new ArrayList<>(1);
    tableKeyList.add("a");
    tableKeyList.add("b");
    tableKeyList.add("c");

    List<String> orderedByList = new ArrayList<>(1);
    orderedByList.add("b ASC");
    orderedByList.add("c DESC");

    String partitionFilter = null;

    String query = BigQuerySinkUtils.generateUpdateUpsertQuery(operation, sourceTableId, destinationTableId,
                                                               tableFieldsList, tableKeyList, orderedByList,
                                                               partitionFilter);

    Assert.assertEquals("MERGE `dummy_dest_project.dummy_dest_dataset.dummy_dest_table` T USING (SELECT * " +
                          "FROM (SELECT row_number() OVER (PARTITION BY `a`, `b`, `c` " +
                          "ORDER BY `b` ASC, `c` DESC) as rowid, * FROM " +
                          "`dummy_src_project.dummy_src_dataset.dummy_src_table`) " +
                          "where rowid = 1) S ON T.`a` = S.`a` AND T.`b` = S.`b` AND T.`c` = S.`c` " +
                          "WHEN NOT MATCHED THEN INSERT (`a`, `b`, `c`) VALUES(`a`, `b`, `c`)", query);
  }
}
