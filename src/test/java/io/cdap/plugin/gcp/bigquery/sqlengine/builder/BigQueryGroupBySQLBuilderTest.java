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

import io.cdap.cdap.etl.api.aggregation.GroupByAggregationDefinition;
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
public class BigQueryGroupBySQLBuilderTest {

  private BigQueryGroupBySQLBuilder helper;
  private SQLExpressionFactory factory;
  private Map<String, Expression> selectFields;
  private List<Expression> groupByFields;
  private GroupByAggregationDefinition def;

  @Before
  public void setUp() {
    factory = new SQLExpressionFactory();
    GroupByAggregationDefinition.Builder builder = GroupByAggregationDefinition.builder();

    // Build aggregation definition
    selectFields = new LinkedHashMap<>();
    selectFields.put("a", factory.compile("a"));
    selectFields.put("b", factory.compile("MAX(b)"));
    selectFields.put("c", factory.compile("MIN(col_c)"));
    selectFields.put("d", factory.compile("d"));

    groupByFields = new ArrayList<>(2);
    groupByFields.add(factory.compile("a"));
    groupByFields.add(factory.compile("d"));

    builder.select(selectFields).groupBy(groupByFields);
    def = builder.build();

    helper = new BigQueryGroupBySQLBuilder(def, "select * from tbl", "ds");
  }

  @Test
  public void testGetQuery() {
    Assert.assertEquals("SELECT a AS `a` , MAX(b) AS `b` , MIN(col_c) AS `c` , d AS `d` "
                          + "FROM ( select * from tbl ) AS `ds` "
                          + "GROUP BY a , d",
                        helper.getQuery());
  }


  @Test
  public void testGetSelectedFields() {
    Assert.assertEquals("a AS `a` , "
                          + "MAX(b) AS `b` , "
                          + "MIN(col_c) AS `c` , "
                          + "d AS `d`",
                        helper.getSelectedFields(selectFields));
  }

  @Test
  public void testGetPartitionByFields() {
    Assert.assertEquals("a , d", helper.getGroupByFields(groupByFields));
  }
//
//  @Test
//  public void testGetRowNumColumn() {
//    Assert.assertEquals("ROW_NUMBER() OVER ( PARTITION BY c , d , e ORDER BY e DESC , f ASC ) AS `rn`",
//                        helper.getRowNumColumn(def));
//  }
//
//  @Test
//  public void testGetPartitionByFields() {
//    Assert.assertEquals("c , d , e", helper.getPartitionByFields(groupByFields));
//  }
//
//  @Test
//  public void testGetOrderByFields() {
//    Assert.assertEquals("e DESC , f ASC", helper.getOrderByFields(filterFields));
//  }
//
//  @Test
//  public void testGetOrderByField() {
//    GroupByAggregationDefinition.FilterExpression maxField1 =
//      new GroupByAggregationDefinition.FilterExpression(factory.compile("field1"),
//                                                            GroupByAggregationDefinition.FilterFunction.MAX);
//    GroupByAggregationDefinition.FilterExpression minField2 =
//      new GroupByAggregationDefinition.FilterExpression(factory.compile("field2"),
//                                                            GroupByAggregationDefinition.FilterFunction.MIN);
//
//    Assert.assertEquals("field1 DESC", helper.getOrderByField(maxField1));
//    Assert.assertEquals("field2 ASC", helper.getOrderByField(minField2));
//  }


}
