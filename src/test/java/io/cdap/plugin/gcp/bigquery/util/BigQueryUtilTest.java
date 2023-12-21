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

package io.cdap.plugin.gcp.bigquery.util;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize.BigNumeric;
import io.cdap.plugin.gcp.bigquery.util.BigQueryTypeSize.Numeric;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;

@RunWith(PowerMockRunner.class)
public class BigQueryUtilTest {

  private static final String BUCKET_PREFIX_ARG = "gcp.bigquery.bucket.prefix";
  MockFailureCollector collector;

  @Before
  public void setUp() {
    collector = new MockFailureCollector();
  }

  @Test
  public void testGetTableSchema() {
    List<Field> fieldList = new ArrayList<>();
    Field boolField = Field.newBuilder("bool", StandardSQLTypeName.BOOL).build();
    fieldList.add(boolField);
    Field bytesField = Field.newBuilder("bytes", StandardSQLTypeName.BYTES).build();
    fieldList.add(bytesField);
    Field dateField = Field.newBuilder("date", StandardSQLTypeName.DATE).build();
    fieldList.add(dateField);
    Field datetimeField = Field.newBuilder("datetime", StandardSQLTypeName.DATETIME).build();
    fieldList.add(datetimeField);
    Field numericField = Field.newBuilder("numeric", StandardSQLTypeName.NUMERIC).build();
    fieldList.add(numericField);
    Field bigNumericField = Field.newBuilder("bignumeric", StandardSQLTypeName.BIGNUMERIC).build();
    fieldList.add(bigNumericField);
    Field float64Field = Field.newBuilder("float64", StandardSQLTypeName.FLOAT64).build();
    fieldList.add(float64Field);
    Field int64Field = Field.newBuilder("int64", StandardSQLTypeName.INT64).build();
    fieldList.add(int64Field);
    Field stringField = Field.newBuilder("string", StandardSQLTypeName.STRING).build();
    fieldList.add(stringField);
    Field timeField = Field.newBuilder("time", StandardSQLTypeName.TIME).build();
    fieldList.add(timeField);
    Field timestampField = Field.newBuilder("timestamp", StandardSQLTypeName.TIMESTAMP).build();
    fieldList.add(timestampField);

    Field subStringField = Field.newBuilder("subStringField", StandardSQLTypeName.STRING).build();
    Field subRecord = Field.newBuilder("subStructure", StandardSQLTypeName.STRUCT, subStringField).build();
    Field recordField = Field.newBuilder("structure", StandardSQLTypeName.STRUCT, subRecord).build();
    fieldList.add(recordField);
    Schema tableSchema = BigQueryUtil.getTableSchema(com.google.cloud.bigquery.Schema.of(fieldList), null);

    List<Schema.Field> cdapFieldList = new ArrayList<>();
    Schema.Field cdapBoolField = Schema.Field.of("bool", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)));
    cdapFieldList.add(cdapBoolField);
    Schema.Field cdapBytesField = Schema.Field.of("bytes", Schema.nullableOf(Schema.of(Schema.Type.BYTES)));
    cdapFieldList.add(cdapBytesField);
    Schema.Field cdapDateField = Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE)));
    cdapFieldList.add(cdapDateField);
    Schema.Field cdapDatetimeField =
      Schema.Field.of("datetime", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME)));
    cdapFieldList.add(cdapDatetimeField);
    Schema.Field cdapNumericField = Schema.Field.of("numeric", Schema.nullableOf(Schema.decimalOf(
        Numeric.PRECISION, Numeric.SCALE)));
    cdapFieldList.add(cdapNumericField);
    Schema.Field cdapBigNumericField = Schema.Field.of("bignumeric", Schema.nullableOf(Schema.decimalOf(
        BigNumeric.PRECISION, BigNumeric.SCALE)));
    cdapFieldList.add(cdapBigNumericField);
    Schema.Field cdapFloat64Field = Schema.Field.of("float64", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)));
    cdapFieldList.add(cdapFloat64Field);
    Schema.Field cdapInt64Field = Schema.Field.of("int64", Schema.nullableOf(Schema.of(Schema.Type.LONG)));
    cdapFieldList.add(cdapInt64Field);
    Schema.Field cdapStringField = Schema.Field.of("string", Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    cdapFieldList.add(cdapStringField);
    Schema.Field cdapTimeField = Schema.Field.of("time", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS)));
    cdapFieldList.add(cdapTimeField);
    Schema.Field cdapTimestampField =
      Schema.Field.of("timestamp", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));
    cdapFieldList.add(cdapTimestampField);

    Schema.Field cdapSubStringField = Schema.Field.of("subStringField",
        Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    Schema.Field cdapSubRecordField = Schema.Field.of("subStructure",
        Schema.nullableOf(Schema.recordOf("structure.subStructure" +
                                            Schema.recordOf(cdapSubStringField).getRecordName(), cdapSubStringField)));
    Schema.Field cdapRecordField = Schema.Field.of("structure",
        Schema.nullableOf(Schema.recordOf("structure" +
          Schema.recordOf(cdapSubRecordField).getRecordName(), cdapSubRecordField)));
    cdapFieldList.add(cdapRecordField);

    Schema expectedSchema = Schema.recordOf("output", cdapFieldList);
    assertEquals(expectedSchema, tableSchema);
  }

  @Test
  public void testValidateArraySchema() {
      String arrayField = "nullArray";
      Schema schema = Schema.recordOf("record",
              Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
              Schema.Field.of(arrayField, Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));

      FailureCollector collector = Mockito.mock(FailureCollector.class);
      Mockito.when(collector.addFailure(anyString(), anyString())).thenReturn(new ValidationFailure("errorMessage"));

      Field bigQueryField = Field.newBuilder(arrayField, StandardSQLTypeName.STRING)
        .setMode(Field.Mode.REPEATED).build();
      Schema.Field recordField = schema.getField(arrayField);

      BigQueryUtil.validateFieldModeMatches(bigQueryField, recordField, false, collector);

      Mockito.verify(collector, Mockito.times(0)).addFailure(anyString(), anyString());
  }

  @Test
  public void testGetBucketPrefix() {
    Map<String, String> args = new HashMap<>();
    String prefix = "this-is-valid-as-a-prefix-to-use-123456789_.abcdef";
    args.put(BUCKET_PREFIX_ARG, prefix);
    Assert.assertEquals(BigQueryUtil.getBucketPrefix(args), prefix);
  }

  @Test
  public void testFormatAsFQNComponentWithReservedCharacters() {
    String input = ":special`chars \t\n";
    String expected = "`:special`chars \t\n`";
    String result = GCPUtils.formatAsFQNComponent(input);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testFormatAsFQNComponentWithoutReservedCharacters() {
    String input = "validComponent";
    String expected = "validComponent";
    String result = GCPUtils.formatAsFQNComponent(input);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testGetFQNWithReservedCharacters() {
    String datasetProject = "google.com:project";
    String datasetName = "dataset";
    String tableName = "table";
    String expectedFQN = "bigquery:`google.com:project`.dataset.table";

    String result = BigQueryUtil.getFQN(datasetProject, datasetName, tableName);
    Assert.assertEquals(result, expectedFQN);
  }

  @Test
  public void testGetFQNWithoutReservedCharacters() {
    String datasetProject = "project";
    String datasetName = "dataset";
    String tableName = "table";
    String expectedFQN = "bigquery:project.dataset.table";

    String result = BigQueryUtil.getFQN(datasetProject, datasetName, tableName);
    Assert.assertEquals(result, expectedFQN);
  }

  @Test
  public void testGetBucketPrefixNotSet() {
    Map<String, String> args = new HashMap<>();
    Assert.assertNull(BigQueryUtil.getBucketPrefix(args));
  }

  @Test
  public void testGetBucketPrefixInvalidBucketName() {
    Map<String, String> args = new HashMap<>();
    args.put(BUCKET_PREFIX_ARG, "This is an invalid bucket name!@");

    IllegalArgumentException e = null;

    try {
      BigQueryUtil.getBucketPrefix(args);
    } catch (IllegalArgumentException ie) {
      e = ie;
    }

    Assert.assertNotNull(e);
    Assert.assertEquals("The configured bucket prefix 'This is an invalid bucket name!@' is not a valid " +
                          "bucket name. Bucket names can only contain lowercase letters, numeric " +
                          "characters, dashes (-), underscores (_), and dots (.).", e.getMessage());
  }

  @Test
  public void testGetBucketPrefixTooLong() {
    Map<String, String> args = new HashMap<>();
    args.put(BUCKET_PREFIX_ARG, "this-prefix-is-too-long-to-be-used-as-a-prefix-oops");

    IllegalArgumentException e = null;

    try {
      BigQueryUtil.getBucketPrefix(args);
    } catch (IllegalArgumentException ie) {
      e = ie;
    }

    Assert.assertNotNull(e);
    Assert.assertEquals("The configured bucket prefix 'this-prefix-is-too-long-to-be-used-as-a-prefix-oops'" +
                          " should be 50 characters or shorter.", e.getMessage());
  }

  @Test
  public void testCRC32LocationDoesNotCollide() {
    // Set containing all current GCP region names.
    Set<String> locations = new HashSet<>();
    locations.add("us");
    locations.add("eu");
    locations.add("asia-east1");
    locations.add("asia-east2");
    locations.add("asia-northeast1");
    locations.add("asia-northeast2");
    locations.add("asia-northeast3");
    locations.add("asia-south1");
    locations.add("asia-south2");
    locations.add("asia-southeast1");
    locations.add("asia-southeast2");
    locations.add("australia-southeast1");
    locations.add("australia-southeast2");
    locations.add("europe-central2");
    locations.add("europe-north1");
    locations.add("europe-southwest1");
    locations.add("europe-west1");
    locations.add("europe-west2");
    locations.add("europe-west3");
    locations.add("europe-west4");
    locations.add("europe-west6");
    locations.add("europe-west8");
    locations.add("europe-west9");
    locations.add("northamerica-northeast1");
    locations.add("northamerica-northeast2");
    locations.add("southamerica-east1");
    locations.add("southamerica-west1");
    locations.add("us-central1");
    locations.add("us-east1");
    locations.add("us-east4");
    locations.add("us-east5");
    locations.add("us-south1");
    locations.add("us-west1");
    locations.add("us-west2");
    locations.add("us-west3");
    locations.add("us-west4");

    // Check there are no collisions
    Set<String> hashValues = new HashSet<>();
    for (String location : locations) {
      String hash = BigQueryUtil.crc32location(location);
      Assert.assertFalse(hashValues.contains(hash));
      hashValues.add(hash);
    }
  }

  @Test
  public void testJobLabelWithDuplicateKeys() {
    String jobLabelKeyValue = "key1:value1,key2:value2,key1:value3";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Duplicate job label key 'key1'.",
            collector.getValidationFailures().get(0).getMessage());
  }
  @Test
  public void testJobLabelWithDuplicateValues() {
    String jobLabelKeyValue = "key1:value1,key2:value2,key3:value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithCapitalLetters() {
    String jobLabelKeyValue = "keY1:value1,key2:value2,key3:value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key 'keY1'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelStartingWithCapitalLetters() {
    String jobLabelKeyValue = "Key1:value1,key2:value2,key3:value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key 'Key1'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithInvalidCharacters() {
    String jobLabelKeyValue = "key1:value1,key2:value2,key3:value1@";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label value 'value1@'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithEmptyKey() {
    String jobLabelKeyValue = ":value1,key2:value2,key3:value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key ''.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithEmptyValue() {
    String jobLabelKeyValue = "key1:,key2:value2,key3:value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithWrongFormat() {
    String jobLabelKeyValue = "key1=value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key 'key1=value1'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithNull() {
    String jobLabelKeyValue = null;
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithReservedKeys() {
    String jobLabelKeyValue = "job_source:value1,type:value2";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(2, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key 'job_source'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWith65Keys() {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= 65; i++) {
      String key = "key" + i;
      String value = "value" + i;
      sb.append(key).append(":").append(value).append(",");
    }
    // remove the last comma
    sb.deleteCharAt(sb.length() - 1);
    Assert.assertEquals(65, sb.toString().split(",").length);
    String jobLabelKeyValue = sb.toString();
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Number of job labels exceeds the limit.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithKeyLength64() {
    String key64 = "1234567890123456789012345678901234567890123456789012345678901234";
    String jobLabelKeyValue = key64 + ":value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key '" + key64 + "'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithValueLength64() {
    String value64 = "1234567890123456789012345678901234567890123456789012345678901234";
    String jobLabelKeyValue = "key1:" + value64;
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label value '" + value64 + "'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithKeyStartingWithNumber() {
    String jobLabelKeyValue = "1key:value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key '1key'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithKeyStartingWithDash() {
    String jobLabelKeyValue = "-key:value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key '-key'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithKeyStartingWithHyphen() {
    String jobLabelKeyValue = "_key:value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key '_key'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithKeyWithChineseCharacter() {
    String jobLabelKeyValue = "中文:value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithKeyWithJapaneseCharacter() {
    String jobLabelKeyValue = "日本語:value1";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithValueStartingWithNumber() {
    String jobLabelKeyValue = "key:1value";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithValueStartingWithDash() {
    String jobLabelKeyValue = "key:-value";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithValueStartingWithCaptialLetter() {
    String jobLabelKeyValue = "key:Value";
    BigQueryUtil.validateJobLabelKeyValue(jobLabelKeyValue, collector, "test");
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label value 'Value'.",
            collector.getValidationFailures().get(0).getMessage());
  }

}
