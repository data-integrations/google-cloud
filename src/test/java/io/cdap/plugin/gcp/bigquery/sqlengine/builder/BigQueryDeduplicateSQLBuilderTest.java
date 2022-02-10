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

package io.cdap.plugin.gcp.bigquery.sqlengine.builder;

import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.plugin.gcp.bigquery.relational.SQLExpressionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryDeduplicateSQLBuilderTest {

  BigQueryDeduplicateSQLBuilder helper;
  SQLExpressionFactory factory;
  Map<String, Expression> selectFields;
  List<Expression> dedupFields;
  List<DeduplicateAggregationDefinition.FilterExpression> filterFields;
  DeduplicateAggregationDefinition def;

  @Before
  public void setUp() {
    factory = new SQLExpressionFactory();
    DeduplicateAggregationDefinition.Builder builder = DeduplicateAggregationDefinition.builder();

    // Build aggregation definition
    selectFields = new LinkedHashMap<>();
    selectFields.put("alias_a", factory.compile("a"));
    selectFields.put("alias_b", factory.compile("b"));
    selectFields.put("c", factory.compile("c"));
    selectFields.put("d", factory.compile("d"));
    selectFields.put("e", factory.compile("e"));
    selectFields.put("f", factory.compile("f"));

    dedupFields = new ArrayList<>(2);
    dedupFields.add(factory.compile("c"));
    dedupFields.add(factory.compile("d"));
    dedupFields.add(factory.compile("e"));

    filterFields = new ArrayList<>(2);
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      factory.compile("e"), DeduplicateAggregationDefinition.FilterFunction.MAX));
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      factory.compile("f"), DeduplicateAggregationDefinition.FilterFunction.MIN));

    builder.select(selectFields).dedupOn(dedupFields).filterDuplicatesBy(filterFields);
    def = builder.build();

    helper = new BigQueryDeduplicateSQLBuilder(def, "select * from tbl", "ds", "rn");
  }

  @Test
  public void testGetQuery() {
    Assert.assertEquals("SELECT * EXCEPT(`rn`) FROM ("
                          + "SELECT "
                          + "a AS `alias_a` , "
                          + "b AS `alias_b` , "
                          + "c AS `c` , "
                          + "d AS `d` , "
                          + "e AS `e` , "
                          + "f AS `f` , "
                          + "ROW_NUMBER() OVER ( PARTITION BY c , d , e ORDER BY e DESC , f ASC ) AS `rn` "
                          + "FROM ( select * from tbl ) AS `ds`"
                          + ") WHERE `rn` = 1",
                        helper.getQuery());
  }

  @Test
  public void testGetInnerSelect() {
    Assert.assertEquals("SELECT "
                          + "a AS `alias_a` , "
                          + "b AS `alias_b` , "
                          + "c AS `c` , "
                          + "d AS `d` , "
                          + "e AS `e` , "
                          + "f AS `f` , "
                          + "ROW_NUMBER() OVER ( PARTITION BY c , d , e ORDER BY e DESC , f ASC ) AS `rn` "
                          + "FROM ( select * from tbl ) AS `ds`",
                        helper.getInnerSelect());
  }

  @Test
  public void testGetSelectedFields() {
    Assert.assertEquals("a AS `alias_a` , "
                          + "b AS `alias_b` , "
                          + "c AS `c` , "
                          + "d AS `d` , "
                          + "e AS `e` , "
                          + "f AS `f` , "
                          + "ROW_NUMBER() OVER ( PARTITION BY c , d , e ORDER BY e DESC , f ASC ) AS `rn`",
                        helper.getSelectedFields(def));
  }

  @Test
  public void testGetRowNumColumn() {
    Assert.assertEquals("ROW_NUMBER() OVER ( PARTITION BY c , d , e ORDER BY e DESC , f ASC ) AS `rn`",
                        helper.getRowNumColumn(def));
  }

  @Test
  public void testGetPartitionByFields() {
    Assert.assertEquals("c , d , e", helper.getPartitionByFields(dedupFields));
  }

  @Test
  public void testGetOrderByFields() {
    Assert.assertEquals("e DESC , f ASC", helper.getOrderByFields(filterFields));
  }

  @Test
  public void testGetOrderByField() {
    DeduplicateAggregationDefinition.FilterExpression maxField1 =
      new DeduplicateAggregationDefinition.FilterExpression(factory.compile("field1"),
                                                            DeduplicateAggregationDefinition.FilterFunction.MAX);
    DeduplicateAggregationDefinition.FilterExpression minField2 =
      new DeduplicateAggregationDefinition.FilterExpression(factory.compile("field2"),
                                                            DeduplicateAggregationDefinition.FilterFunction.MIN);

    Assert.assertEquals("field1 DESC", helper.getOrderByField(maxField1));
    Assert.assertEquals("field2 ASC", helper.getOrderByField(minField2));
  }


}
