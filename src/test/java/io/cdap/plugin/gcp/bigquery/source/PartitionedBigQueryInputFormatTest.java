/*
 * Copyright © 2020 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.plugin.gcp.bigquery.source;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PartitionedBigQueryInputFormatTest {

  private static final String TEST_PROJECT = "test-project";
  private static final String TEST_DATASET = "test-dataset";
  private static final String TEST_TABLE = "test-table";
  private static final String TEST_FILTER = "age > 10";
  private static final String TEST_LIMIT = "100";
  private static final String TEST_ORDER_BY = "name asc";
  private static final String TEST_TABLE_SPEC = String.format("%s.%s.%s", TEST_PROJECT,
      TEST_DATASET, TEST_TABLE);
  private static final String TEST_FROM_DATE = "2025-01-01";
  private static final String TEST_TO_DATE = "2025-01-02";
  private static final String TEST_PARTITION_CONDITION =
      "TIMESTAMP(`_PARTITIONTIME`) >= TIMESTAMP(\"2025-01-01\") and "
          + "TIMESTAMP(`_PARTITIONTIME`) < TIMESTAMP(\"2025-01-02\")";
  private static final String TEST_DEFAULT_TIME_CONDITION =
      "(`_PARTITIONTIME` IS NOT NULL OR `_PARTITIONTIME` IS NULL)";
  private static final String TEST_DEFAULT_RANGE_CONDITION = "(`range_col` IS NOT NULL "
      + "OR `range_col` IS NULL)";
  private static final String TEST_TIME_UNIT_COL = "my_date_col";
  private static final String TEST_TIME_UNIT_PARTITION_CONDITION =
      "TIMESTAMP(`my_date_col`) >= TIMESTAMP(\"2025-01-01\") and "
          + "TIMESTAMP(`my_date_col`) < TIMESTAMP(\"2025-01-02\")";
  private static final String TEST_DEFAULT_TIME_UNIT_CONDITION =
      "(`my_date_col` IS NOT NULL OR `my_date_col` IS NULL)";


  @Mock
  private StandardTableDefinition mockTableDefinition;
  @Mock
  private TimePartitioning mockTimePartitioning;
  @Mock
  private RangePartitioning mockRangePartitioning;
  @Mock
  private Schema mockSchema;
  @Mock
  private FieldList mockFieldList;
  @Mock
  private Field mockField;

  private PartitionedBigQueryInputFormat format;

  @Before
  public void setUp() {
    format = new PartitionedBigQueryInputFormat();
    when(mockTableDefinition.getTimePartitioning()).thenReturn(null);
    when(mockTableDefinition.getRangePartitioning()).thenReturn(null);
  }

  public void testGenerateQueryForMaterializingView_WithFilterOnly() {
    String expectedQuery = String.format("select * from `%s.%s.%s` where %s",
        TEST_PROJECT, TEST_DATASET, TEST_TABLE, TEST_FILTER);
    String generatedQuery = format.generateQueryForMaterializingView(
        TEST_PROJECT, TEST_DATASET, TEST_TABLE, TEST_FILTER, null, null);

    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQueryForMaterializingView_NoFilterOrOptions() {
    String expectedQuery = String.format("select * from `%s.%s.%s`",
        TEST_PROJECT, TEST_DATASET, TEST_TABLE);

    String generatedQuery = format.generateQueryForMaterializingView(
        TEST_PROJECT, TEST_DATASET, TEST_TABLE, null, null, null);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQueryForMaterializingView_AllOptions() {
    String expectedQuery = String.format("select * from `%s.%s.%s` where %s order by %s limit %s",
        TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        TEST_FILTER, TEST_ORDER_BY, TEST_LIMIT);

    String generatedQuery = format.generateQueryForMaterializingView(
        TEST_PROJECT, TEST_DATASET, TEST_TABLE, TEST_FILTER, TEST_LIMIT, TEST_ORDER_BY);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_WithFilterOnly() {
    String expectedQuery = String.format("select * from %s where %s",
        TEST_TABLE_SPEC, TEST_FILTER);

    String generatedQuery = format.generateQuery(null, null,
        TEST_FILTER, TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        null, null,
        false, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_AllOptions() {
    String expectedQuery = String.format("select * from %s where %s order by %s limit %s",
        TEST_TABLE_SPEC, TEST_FILTER, TEST_ORDER_BY, TEST_LIMIT);

    String generatedQuery = format.generateQuery(null, null,
        TEST_FILTER, TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        TEST_LIMIT, TEST_ORDER_BY,
        false, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_TimePartitionWithDates() {
    when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
    when(mockTimePartitioning.getField()).thenReturn(null);

    String expectedQuery = String.format("select * from %s where (%s)",
        TEST_TABLE_SPEC,
        TEST_PARTITION_CONDITION);

    String generatedQuery = format.generateQuery(TEST_FROM_DATE, TEST_TO_DATE, null,
        TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        null, null,
        false, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_TimePartitionRequiredAndFilter() {
    when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
    when(mockTimePartitioning.getField()).thenReturn(null);
    String expectedQuery = String.format("select * from %s where %s and (%s)",
        TEST_TABLE_SPEC, TEST_DEFAULT_TIME_CONDITION, TEST_FILTER);

    String generatedQuery = format.generateQuery(null, null,
        TEST_FILTER, TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        null, null,
        true, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_TimeUnitPartitionWithDates() {
    when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
    when(mockTimePartitioning.getField()).thenReturn(TEST_TIME_UNIT_COL);
    when(mockTableDefinition.getSchema()).thenReturn(mockSchema);
    when(mockSchema.getFields()).thenReturn(mockFieldList);
    when(mockFieldList.get(TEST_TIME_UNIT_COL)).thenReturn(mockField);
    when(mockField.getType()).thenReturn(LegacySQLTypeName.DATE);

    String expectedQuery = String.format("select * from %s where (%s)",
        TEST_TABLE_SPEC, TEST_TIME_UNIT_PARTITION_CONDITION);

    String generatedQuery = format.generateQuery(TEST_FROM_DATE, TEST_TO_DATE, null,
        TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        null, null,
        false, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_TimePartitionFilterNotRequiredWithDates() {
    when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
    when(mockTimePartitioning.getField()).thenReturn(null);

    String expectedQuery = String.format("select * from %s where (%s)",
        TEST_TABLE_SPEC,
        TEST_PARTITION_CONDITION);

    String generatedQuery = format.generateQuery(TEST_FROM_DATE, TEST_TO_DATE, null,
        TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        null, null,
        false, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_NoOptions_ShouldReturnNull() {
    String generatedQuery = format.generateQuery(null, null,
        null, TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        null, null,
        false, mockTableDefinition);
    Assert.assertNull("Query should be null if no filters or options are set.", generatedQuery);
  }

  @Test
  public void testGenerateQuery_WithLimitOnly_ShouldAssertQuery() {
    String expectedQuery = String.format("select * from %s limit %s", TEST_TABLE_SPEC,
        TEST_LIMIT);

    String generatedQuery = format.generateQuery(null, null,
        null, TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        TEST_LIMIT, null,
        false, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_WithOrderByOnly_ShouldAssertQuery() {
    String expectedQuery = String.format("select * from %s order by %s", TEST_TABLE_SPEC,
        TEST_ORDER_BY);

    String generatedQuery = format.generateQuery(null, null,
        null, TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        null, TEST_ORDER_BY,
        false, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_TimePartitionNotRequired_WithDates_ShouldAssertQuery() {
    when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
    when(mockTimePartitioning.getField()).thenReturn(null);

    String expectedQuery = String.format("select * from %s where (%s)",
        TEST_TABLE_SPEC,
        TEST_PARTITION_CONDITION);

    String generatedQuery = format.generateQuery(TEST_FROM_DATE, TEST_TO_DATE, null,
        TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        null, null,
        false, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_TimePartitionRequired_WithFilterOnly_ShouldAssertQuery() {
    when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
    when(mockTimePartitioning.getField()).thenReturn(null);

    String expectedQuery = String.format("select * from %s where %s and (%s)",
        TEST_TABLE_SPEC, TEST_DEFAULT_TIME_CONDITION, TEST_FILTER);

    String generatedQuery = format.generateQuery(null, null,
        TEST_FILTER, TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        null, null,
        true, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_RangePartitionRequiredAndFilter() {
    when(mockTableDefinition.getRangePartitioning()).thenReturn(mockRangePartitioning);
    when(mockRangePartitioning.getField()).thenReturn("range_col");

    String expectedQuery = String.format("select * from %s where %s and (%s)",
        TEST_TABLE_SPEC, TEST_DEFAULT_RANGE_CONDITION, TEST_FILTER);

    String generatedQuery = format.generateQuery(null, null, TEST_FILTER,
        TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        null, null,
        true, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_RangePartitionRequiredWithLimit() {
    when(mockTableDefinition.getRangePartitioning()).thenReturn(mockRangePartitioning);
    when(mockRangePartitioning.getField()).thenReturn("range_col");

    String expectedQuery = String.format("select * from %s where %s limit %s",
        TEST_TABLE_SPEC, TEST_DEFAULT_RANGE_CONDITION, TEST_LIMIT);

    String generatedQuery = format.generateQuery(null, null, null,
        TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        TEST_LIMIT, null,
        true, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_TimeUnitPartitionRequiredAndFilter() {
    when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
    when(mockTimePartitioning.getField()).thenReturn(TEST_TIME_UNIT_COL);

    String expectedQuery = String.format("select * from %s where %s and (%s)",
        TEST_TABLE_SPEC, TEST_DEFAULT_TIME_UNIT_CONDITION, TEST_FILTER);

    String generatedQuery = format.generateQuery(null, null, TEST_FILTER,
        TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        null, null,
        true, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery_TimeUnitPartitionRequiredWithLimit() {
    when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
    when(mockTimePartitioning.getField()).thenReturn(TEST_TIME_UNIT_COL);

    String expectedQuery = String.format("select * from %s where %s limit %s",
        TEST_TABLE_SPEC, TEST_DEFAULT_TIME_UNIT_CONDITION, TEST_LIMIT);

    String generatedQuery = format.generateQuery(null, null, null,
        TEST_PROJECT, TEST_DATASET, TEST_TABLE,
        TEST_LIMIT, null,
        true, mockTableDefinition);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }
}
