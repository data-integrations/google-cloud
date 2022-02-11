/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.relational;

import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.aggregation.GroupByAggregationDefinition;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.plugin.gcp.bigquery.sqlengine.BigQuerySQLDataset;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class
BigQueryRelationTest {

  private BigQueryRelation baseRelation;
  private SQLExpressionFactory factory;

  @Before
  public void setUp() {
    factory = new SQLExpressionFactory();

    BigQuerySQLDataset ds = mock(BigQuerySQLDataset.class);
    Set<String> columns = new LinkedHashSet<>();
    columns.add("a");
    columns.add("b");

    baseRelation = new BigQueryRelation(ds, columns, null, "select * from tbl");
  }

  @Test
  public void testSetColumn() {
    Relation rel = baseRelation.setColumn("c", factory.compile("a+b"));
    Assert.assertTrue(rel instanceof BigQueryRelation);
    Assert.assertEquals(baseRelation, ((BigQueryRelation) rel).getParent());

    Set<String> columns = ((BigQueryRelation) rel).getColumns();
    Assert.assertEquals(3, columns.size());
    Assert.assertTrue(columns.contains("a"));
    Assert.assertTrue(columns.contains("b"));
    Assert.assertTrue(columns.contains("c"));
  }

  @Test
  public void testSetInvalidColumn() {
    Relation rel;

    rel = baseRelation.setColumn("c", new InvalidSQLExpression("c"));
    Assert.assertTrue(rel instanceof InvalidRelation);

    rel = baseRelation.setColumn("c", new NonSQLExpression());
    Assert.assertTrue(rel instanceof InvalidRelation);
  }

  @Test
  public void testDropColumn() {
    Relation rel = baseRelation.dropColumn("b");
    Assert.assertTrue(rel instanceof BigQueryRelation);
    Assert.assertEquals(baseRelation, ((BigQueryRelation) rel).getParent());

    Set<String> columns = ((BigQueryRelation) rel).getColumns();
    Assert.assertEquals(1, columns.size());
    Assert.assertTrue(columns.contains("a"));
  }

  @Test
  public void testDropNonExistingColumn() {
    Relation rel = baseRelation.dropColumn("z");
    Assert.assertTrue(rel instanceof InvalidRelation);
  }

  @Test
  public void testSelect() {
    Map<String, Expression> selectColumns = new LinkedHashMap<>();
    selectColumns.put("new_a", factory.compile("a"));
    selectColumns.put("new_b", factory.compile("b"));

    Relation rel = baseRelation.select(selectColumns);
    Assert.assertTrue(rel instanceof BigQueryRelation);
    Assert.assertEquals(baseRelation, ((BigQueryRelation) rel).getParent());

    Set<String> columns = ((BigQueryRelation) rel).getColumns();
    Assert.assertEquals(2, columns.size());
    Assert.assertTrue(columns.contains("new_a"));
    Assert.assertTrue(columns.contains("new_b"));
  }

  @Test
  public void testInvalidSelect() {
    Relation rel;

    rel = baseRelation.select(Collections.singletonMap("c", new InvalidSQLExpression("c")));
    Assert.assertTrue(rel instanceof InvalidRelation);

    rel = baseRelation.select(Collections.singletonMap("c", new NonSQLExpression()));
    Assert.assertTrue(rel instanceof InvalidRelation);
  }

  @Test
  public void testFilter() {
    Expression filter = factory.compile("a > 2");

    Relation rel = baseRelation.filter(filter);
    Assert.assertTrue(rel instanceof BigQueryRelation);
    Assert.assertEquals(baseRelation, ((BigQueryRelation) rel).getParent());

    Set<String> columns = ((BigQueryRelation) rel).getColumns();
    Assert.assertEquals(2, columns.size());
    Assert.assertTrue(columns.contains("a"));
    Assert.assertTrue(columns.contains("b"));
  }

  @Test
  public void testInvalidFilter() {
    Relation rel;

    rel = baseRelation.filter(new InvalidSQLExpression("c"));
    Assert.assertTrue(rel instanceof InvalidRelation);

    rel = baseRelation.filter(new NonSQLExpression());
    Assert.assertTrue(rel instanceof InvalidRelation);
  }

  @Test
  public void testGroupBy() {
    GroupByAggregationDefinition def;
    GroupByAggregationDefinition.Builder builder;
    Map<String, Expression> selectFields;
    List<Expression> groupByFields;
    Relation rel;

    // Create builder for aggregation definitions
    builder = new GroupByAggregationDefinition.Builder();
    // Build aggregation definition
    selectFields = new LinkedHashMap<>();
    selectFields.put("a", factory.compile("a"));
    selectFields.put("b", factory.compile("MAX(a)"));
    selectFields.put("c", factory.compile("MIN(b)"));
    selectFields.put("d", factory.compile("d"));
    // Build aggregation definition
    groupByFields = new ArrayList<>(2);
    groupByFields.add(factory.compile("a"));
    groupByFields.add(factory.compile("d"));

    builder.select(selectFields).groupBy(groupByFields);
    def = builder.build();

    rel = baseRelation.groupBy(def);
    Assert.assertTrue(rel instanceof BigQueryRelation);
    Assert.assertEquals(baseRelation, ((BigQueryRelation) rel).getParent());

    Set<String> columns = ((BigQueryRelation) rel).getColumns();
    Assert.assertEquals(4, columns.size());
    Assert.assertTrue(columns.contains("a"));
    Assert.assertTrue(columns.contains("b"));
    Assert.assertTrue(columns.contains("c"));
    Assert.assertTrue(columns.contains("d"));
  }

  @Test
  public void testInvalidGroupBy() {
    GroupByAggregationDefinition def;
    GroupByAggregationDefinition.Builder builder;
    Map<String, Expression> selectFields;
    List<Expression> groupByFields;
    Relation rel;

    // Create builder for aggregation definitions
    builder = new GroupByAggregationDefinition.Builder();
    // Build aggregation definition
    selectFields = new LinkedHashMap<>();
    selectFields.put("a", new InvalidSQLExpression("a"));
    // Build aggregation definition
    groupByFields = new ArrayList<>(2);
    groupByFields.add(factory.compile("a"));

    builder.select(selectFields).groupBy(groupByFields);
    def = builder.build();

    rel = baseRelation.groupBy(def);
    Assert.assertTrue(rel instanceof InvalidRelation);
  }

  @Test
  public void testDeduplicate() {
    DeduplicateAggregationDefinition def;
    DeduplicateAggregationDefinition.Builder builder;
    Map<String, Expression> selectFields;
    List<Expression> dedupFields;
    List<DeduplicateAggregationDefinition.FilterExpression> filterFields;
    Relation rel;

    // Create builder for aggregation definitions
    builder = new DeduplicateAggregationDefinition.Builder();
    // Build aggregation definition
    selectFields = new LinkedHashMap<>();
    selectFields.put("a", factory.compile("a"));
    selectFields.put("b", factory.compile("b"));
    selectFields.put("c", factory.compile("c"));
    selectFields.put("d", factory.compile("d"));
    // Build aggregation definition
    dedupFields = new ArrayList<>(2);
    dedupFields.add(factory.compile("a"));
    dedupFields.add(factory.compile("d"));
    // Build FilterFIelds definition
    filterFields = new ArrayList<>(2);
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      factory.compile("a"), DeduplicateAggregationDefinition.FilterFunction.MAX));

    builder.select(selectFields).dedupOn(dedupFields).filterDuplicatesBy(filterFields);
    def = builder.build();

    rel = baseRelation.deduplicate(def);
    Assert.assertTrue(rel instanceof BigQueryRelation);
    Assert.assertEquals(baseRelation, ((BigQueryRelation) rel).getParent());

    Set<String> columns = ((BigQueryRelation) rel).getColumns();
    Assert.assertEquals(4, columns.size());
    Assert.assertTrue(columns.contains("a"));
    Assert.assertTrue(columns.contains("b"));
    Assert.assertTrue(columns.contains("c"));
    Assert.assertTrue(columns.contains("d"));
  }

  @Test
  public void testInvalidDeduplicate() {
    DeduplicateAggregationDefinition def;
    DeduplicateAggregationDefinition.Builder builder;
    Map<String, Expression> selectFields;
    List<Expression> dedupFields;
    List<DeduplicateAggregationDefinition.FilterExpression> filterFields;
    Relation rel;

    // Create builder for aggregation definitions
    builder = new DeduplicateAggregationDefinition.Builder();
    // Build aggregation definition
    selectFields = new LinkedHashMap<>();
    selectFields.put("a", new InvalidSQLExpression("a"));
    // Build aggregation definition
    dedupFields = new ArrayList<>(1);
    dedupFields.add(factory.compile("a"));
    // Build FilterFIelds definition
    filterFields = new ArrayList<>(1);
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      factory.compile("a"), DeduplicateAggregationDefinition.FilterFunction.MAX));

    builder.select(selectFields).dedupOn(dedupFields).filterDuplicatesBy(filterFields);
    def = builder.build();

    rel = baseRelation.deduplicate(def);
    Assert.assertTrue(rel instanceof InvalidRelation);
  }

  @Test
  public void testSupportsGroupByAggregationDefinition() {
    Map<String, Expression> selectFields = new LinkedHashMap<>();
    List<Expression> groupByFields = new ArrayList<>(1);

    // Set up mocks
    GroupByAggregationDefinition def = mock(GroupByAggregationDefinition.class);
    when(def.getSelectExpressions()).thenReturn(selectFields);
    when(def.getGroupByExpressions()).thenReturn(groupByFields);

    // Check valid definition
    selectFields.put("a", factory.compile("a"));
    groupByFields.add(factory.compile("a"));
    Assert.assertTrue(baseRelation.supportsGroupByAggregationDefinition(def));
    selectFields.clear();
    groupByFields.clear();

    // Check invalid select field
    selectFields.put("a", new InvalidSQLExpression("a"));
    groupByFields.add(factory.compile("a"));
    Assert.assertFalse(baseRelation.supportsGroupByAggregationDefinition(def));
    selectFields.clear();
    groupByFields.clear();

    // Check unsupported Select field
    selectFields.put("a", new NonSQLExpression());
    groupByFields.add(factory.compile("a"));
    Assert.assertFalse(baseRelation.supportsGroupByAggregationDefinition(def));
    selectFields.clear();
    groupByFields.clear();

    // Check invalid groupByField field
    selectFields.put("a", factory.compile("a"));
    groupByFields.add(new InvalidSQLExpression("a"));
    Assert.assertFalse(baseRelation.supportsGroupByAggregationDefinition(def));
    selectFields.clear();
    groupByFields.clear();

    // Check unsupported groupByField field
    selectFields.put("a", factory.compile("a"));
    groupByFields.add(new NonSQLExpression());
    Assert.assertFalse(baseRelation.supportsGroupByAggregationDefinition(def));
    selectFields.clear();
    groupByFields.clear();
  }

  @Test
  public void testSupportsDeduplicateAggregationDefinition() {
    Map<String, Expression> selectFields = new LinkedHashMap<>();
    List<Expression> dedupFields = new ArrayList<>(1);
    List<DeduplicateAggregationDefinition.FilterExpression> filterFields = new ArrayList<>(1);

    // Set up mocks
    DeduplicateAggregationDefinition def = mock(DeduplicateAggregationDefinition.class);
    when(def.getSelectExpressions()).thenReturn(selectFields);
    when(def.getGroupByExpressions()).thenReturn(dedupFields);
    when(def.getFilterExpressions()).thenReturn(filterFields);

    // Check valid definition
    selectFields.put("a", factory.compile("a"));
    dedupFields.add(factory.compile("a"));
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      factory.compile("a"), DeduplicateAggregationDefinition.FilterFunction.MAX));
    Assert.assertTrue(baseRelation.supportsDeduplicateAggregationDefinition(def));
    selectFields.clear();
    dedupFields.clear();
    filterFields.clear();


    // Check invalid select field
    selectFields.put("a", new InvalidSQLExpression("a"));
    dedupFields.add(factory.compile("a"));
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      factory.compile("a"), DeduplicateAggregationDefinition.FilterFunction.MAX));
    Assert.assertFalse(baseRelation.supportsDeduplicateAggregationDefinition(def));
    selectFields.clear();
    dedupFields.clear();
    filterFields.clear();

    // Check unsupported Select field
    selectFields.put("a", new NonSQLExpression());
    dedupFields.add(factory.compile("a"));
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      factory.compile("a"), DeduplicateAggregationDefinition.FilterFunction.MAX));
    Assert.assertFalse(baseRelation.supportsDeduplicateAggregationDefinition(def));
    selectFields.clear();
    dedupFields.clear();
    filterFields.clear();

    // Check invalid deduplication field
    selectFields.put("a", factory.compile("a"));
    dedupFields.add(new InvalidSQLExpression("a"));
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      factory.compile("a"), DeduplicateAggregationDefinition.FilterFunction.MAX));
    Assert.assertFalse(baseRelation.supportsDeduplicateAggregationDefinition(def));
    selectFields.clear();
    dedupFields.clear();
    filterFields.clear();

    // Check unsupported deduplication field
    selectFields.put("a", factory.compile("a"));
    dedupFields.add(new NonSQLExpression());
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      factory.compile("a"), DeduplicateAggregationDefinition.FilterFunction.MAX));
    Assert.assertFalse(baseRelation.supportsDeduplicateAggregationDefinition(def));
    selectFields.clear();
    dedupFields.clear();
    filterFields.clear();

    // Check invalid filter field
    selectFields.put("a", factory.compile("a"));
    dedupFields.add(factory.compile("a"));
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      new InvalidSQLExpression("a"), DeduplicateAggregationDefinition.FilterFunction.MAX));
    Assert.assertFalse(baseRelation.supportsDeduplicateAggregationDefinition(def));
    selectFields.clear();
    dedupFields.clear();
    filterFields.clear();

    // Check unsupported filter field
    selectFields.put("a", factory.compile("a"));
    dedupFields.add(factory.compile("a"));
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      new NonSQLExpression(), DeduplicateAggregationDefinition.FilterFunction.MAX));
    Assert.assertFalse(baseRelation.supportsDeduplicateAggregationDefinition(def));
    selectFields.clear();
    dedupFields.clear();
    filterFields.clear();
  }

  @Test
  public void testSupportsExpressions() {
    List<Expression> expressions = new ArrayList<>(2);
    expressions.add(factory.compile("a"));
    Assert.assertTrue(baseRelation.supportsExpressions(expressions));

    expressions.add(new InvalidSQLExpression("a"));
    Assert.assertFalse(baseRelation.supportsExpressions(expressions));

    expressions.remove(1);
    expressions.add(new NonSQLExpression());
    Assert.assertFalse(baseRelation.supportsExpressions(expressions));

    expressions.remove(1);
    Assert.assertTrue(baseRelation.supportsExpressions(expressions));
  }

  @Test
  public void testSupportsExpression() {
    Assert.assertTrue(baseRelation.supportsExpression(factory.compile("a")));
    Assert.assertFalse(baseRelation.supportsExpression(new InvalidSQLExpression("a")));
    Assert.assertFalse(baseRelation.supportsExpression(new NonSQLExpression()));
  }

  /**
   * Invalid SQL expression with the correct class
   */
  private static class InvalidSQLExpression extends SQLExpression {
    public InvalidSQLExpression(String expression) {
      super(expression);
    }

    @Override
    public boolean isValid() {
      return false;
    }
  }
  /**
   * Expression class that doesn't extend from SQLExpression
   */
  private static class NonSQLExpression implements Expression {
    @Override
    public boolean isValid() {
      return true;
    }

    @Override
    public String getValidationError() {
      return null;
    }
  }
}
