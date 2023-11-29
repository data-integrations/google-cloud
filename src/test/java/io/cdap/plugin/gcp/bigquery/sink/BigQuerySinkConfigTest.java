/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.cloud.bigquery.TimePartitioning;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link BigQuerySinkConfig}.
 */

public class BigQuerySinkConfigTest {
  MockFailureCollector collector;
  BigQuerySinkConfig config;
  BigQuerySinkConfig.Builder configBuilder;
  Method validateTimePartitioningColumnMethod;
  Map<String, String> arguments;
  private static final String invalidTableNameErrorMessage =
          "Table name can only contain letters (lower or uppercase), numbers, '_' and '-'.";

  @Before
  public void setup() throws NoSuchMethodException {
    collector = new MockFailureCollector();
    config = BigQuerySinkConfig.builder().build();
    configBuilder = BigQuerySinkConfig.builder();
    validateTimePartitioningColumnMethod = config.getClass()
            .getDeclaredMethod("validateTimePartitioningColumn", String.class, FailureCollector.class,
                    Schema.class, TimePartitioning.Type.class);
    validateTimePartitioningColumnMethod.setAccessible(true);
    arguments = new HashMap<>();
  }

  @Test
  public void testValidateTimePartitioningColumnWithHourAndDate() throws
          InvocationTargetException, IllegalAccessException {
    String columnName = "partitionFrom";
    Schema fieldSchema = Schema.recordOf("test", Schema.Field.of("partitionFrom",
            Schema.of(Schema.LogicalType.DATE)));
    TimePartitioning.Type timePartitioningType = TimePartitioning.Type.HOUR;

    validateTimePartitioningColumnMethod.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
    Assert.assertEquals(String.format("Partition column '%s' is of invalid type '%s'.",
                    columnName, fieldSchema.getDisplayName()),
            collector.getValidationFailures().stream().findFirst().get().getMessage());
  }

  @Test
  public void testValidateTimePartitioningColumnWithHourAndTimestamp() throws
          InvocationTargetException, IllegalAccessException {

    String columnName = "partitionFrom";
    Schema schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);

    Schema fieldSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    TimePartitioning.Type timePartitioningType = TimePartitioning.Type.HOUR;

    validateTimePartitioningColumnMethod.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateTimePartitioningColumnWithDayAndString() throws
          InvocationTargetException, IllegalAccessException {

    String columnName = "partitionFrom";
    Schema schema = Schema.of(Schema.Type.STRING);

    Schema fieldSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    TimePartitioning.Type timePartitioningType = TimePartitioning.Type.DAY;

    validateTimePartitioningColumnMethod.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
    Assert.assertEquals(String.format("Partition column '%s' is of invalid type '%s'.",
                    columnName, fieldSchema.getDisplayName()),
            collector.getValidationFailures().stream().findFirst().get().getMessage());
  }

  @Test
  public void testValidateTimePartitioningColumnWithDayAndDate() throws
          InvocationTargetException, IllegalAccessException {

    String columnName = "partitionFrom";
    Schema schema = Schema.of(Schema.LogicalType.DATE);

    Schema fieldSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    TimePartitioning.Type timePartitioningType = TimePartitioning.Type.DAY;

    validateTimePartitioningColumnMethod.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateTimePartitioningColumnWithNullAndDate() throws
          InvocationTargetException, IllegalAccessException {

    String columnName = "partitionFrom";
    Schema schema = Schema.of(Schema.LogicalType.DATE);

    Schema fieldSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    TimePartitioning.Type timePartitioningType = null;

    validateTimePartitioningColumnMethod.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
    // No error as null time timePartitioningType will default to DAY
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithDuplicateKeys() {
    config.jobLabelKeyValue = "key1:value1,key2:value2,key1:value3";
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Duplicate job label key 'key1'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithDuplicateValues() {
    config.jobLabelKeyValue = "key1:value1,key2:value2,key3:value1";
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithCapitalLetters() {
    config.jobLabelKeyValue = "keY1:value1,key2:value2,key3:value1";
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key 'keY1'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelStartingWithCapitalLetters() {
    config.jobLabelKeyValue = "Key1:value1,key2:value2,key3:value1";
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key 'Key1'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithInvalidCharacters() {
    config.jobLabelKeyValue = "key1:value1,key2:value2,key3:value1@";
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label value 'value1@'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithEmptyKey() {
    config.jobLabelKeyValue = ":value1,key2:value2,key3:value1";
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key ''.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithEmptyValue() {
    config.jobLabelKeyValue = "key1:,key2:value2,key3:value1";
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithWrongFormat() {
    config.jobLabelKeyValue = "key1=value1";
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key 'key1=value1'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithNull() {
    config.jobLabelKeyValue = null;
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithReservedKeys() {
    config.jobLabelKeyValue = "job_source:value1,type:value2";
    config.validate(collector);
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
    config.jobLabelKeyValue = sb.toString();
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Number of job labels exceeds the limit.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithKeyLength64() {
    String key64 = "1234567890123456789012345678901234567890123456789012345678901234";
    config.jobLabelKeyValue = key64 + ":value1";
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key '" + key64 + "'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithValueLength64() {
    String value64 = "1234567890123456789012345678901234567890123456789012345678901234";
    config.jobLabelKeyValue = "key1:" + value64;
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label value '" + value64 + "'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithKeyStartingWithNumber() {
    config.jobLabelKeyValue = "1key:value1";
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key '1key'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithKeyStartingWithDash() {
    config.jobLabelKeyValue = "-key:value1";
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key '-key'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithKeyStartingWithHyphen() {
    config.jobLabelKeyValue = "_key:value1";
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label key '_key'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testJobLabelWithKeyWithChineseCharacter() {
    config.jobLabelKeyValue = "中文:value1";
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithKeyWithJapaneseCharacter() {
    config.jobLabelKeyValue = "日本語:value1";
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithValueStartingWithNumber() {
    config.jobLabelKeyValue = "key:1value";
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithValueStartingWithDash() {
    config.jobLabelKeyValue = "key:-value";
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testJobLabelWithValueStartingWithCaptialLetter() {
    config.jobLabelKeyValue = "key:Value";
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Invalid job label value 'Value'.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateColumnNameWithValidColumnName() {
    String columnName = "test";
    Schema schema = Schema.recordOf("test", Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateColumnNameWithChineseColumnName() {
    String columnName = "测试";
    Schema schema = Schema.recordOf("test", Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateColumnNameWithInvalidColumnName() {
    String columnName = "test@";
    Schema schema = Schema.recordOf("test", Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Output field 'test@' contains invalid characters. " +
            "Check column names docs for more details.", collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateColumnNameWithJapaneseColumnName() {
    String columnName = "テスト";
    Schema schema = Schema.recordOf("test", Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateColumnNameWithSpace() {
    String columnName = "test test";
    Schema schema = Schema.recordOf("test", Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateColumnNameWithEmoji() {
    String columnName = "test😀";
    Schema schema = Schema.recordOf("test", Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Output field 'test😀' contains invalid characters. " +
            "Check column names docs for more details.", collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateColumnNameWithUnderscore() {
    String columnName = "test_test";
    Schema schema = Schema.recordOf("test", Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateColumnNameWithDash() {
    String columnName = "test-test";
    Schema schema = Schema.recordOf("test", Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateColumnNameWithCapitalLetters() {
    String columnName = "Test";
    Schema schema = Schema.recordOf("test", Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateColumnNameWithNumbers() {
    String columnName = "1234";
    Schema schema = Schema.recordOf("test", Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateColumnNameWithSpecialCharacter() {
    String columnName = "test!";
    Schema schema = Schema.recordOf("test", Schema.Field.of(columnName, Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Output field 'test!' contains invalid characters. " +
            "Check column names docs for more details.", collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateColumnNameWith300Length() {
    String columnName = "a";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 300; i++) {
      sb.append(columnName);
    }
    Schema schema = Schema.recordOf("test", Schema.Field.of(sb.toString(), Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateColumnNameWith301Length() {
    String columnName = "a";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 301; i++) {
      sb.append(columnName);
    }
    Schema schema = Schema.recordOf("test", Schema.Field.of(sb.toString(), Schema.of(Schema.Type.STRING)));
    config.validate(schema, schema, collector, arguments);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Output field '" + sb + "' exceeds the maximum length of 300 characters.",
            collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateTableNameWithValidTableName() {
    List<String> validTableNames = Arrays.asList("my_table", "users", "customer_info",
            "_internal_data", "orders2023", "2023_sales", "テスト", "测试", "हैलो वर्ल्ड");
    for (String tableName : validTableNames) {
      config = configBuilder.setTable(tableName).build();
      config.validate(collector);
      Assert.assertEquals(0, collector.getValidationFailures().size());
    }
  }

  @Test
  public void testValidateTableNameWithInvalidTableName() {
    List<String> invalidTableNames = Arrays.asList("!@#$%", "my😀table", "user/role", "data/tab",
            "(invalid)", "my_table;");
    for (String tableName : invalidTableNames) {
      collector = new MockFailureCollector();
      config = configBuilder.setTable(tableName).build();
      config.validate(collector);
      Assert.assertEquals(1, collector.getValidationFailures().size());
      Assert.assertEquals(invalidTableNameErrorMessage, collector.getValidationFailures().get(0).getMessage());
    }
  }
}
