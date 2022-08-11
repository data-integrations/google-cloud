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
 * License for the specific language govethe_row_numbering permissions and limitations under
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

  private BigQueryDeduplicateSQLBuilder helper;
  private SQLExpressionFactory factory;
  private Map<String, Expression> selectFields;
  private List<Expression> dedupFields;
  private List<DeduplicateAggregationDefinition.FilterExpression> filterFields;
  private DeduplicateAggregationDefinition fullDefinition;
  private DeduplicateAggregationDefinition onlyDedupFieldsDefinition;

  @Before
  public void setUp() {
    factory = new SQLExpressionFactory();

    // Build aggregation definition
    selectFields = new LinkedHashMap<>();
    selectFields.put("alias_a", factory.compile("a"));
    selectFields.put("alias_b", factory.compile("b"));
    selectFields.put("c", factory.compile("c"));
    selectFields.put("d", factory.compile("d"));
    selectFields.put("e", factory.compile("e"));
    selectFields.put("f", factory.compile("f"));

    dedupFields = new ArrayList<>(3);
    dedupFields.add(factory.compile("c"));
    dedupFields.add(factory.compile("d"));
    dedupFields.add(factory.compile("e"));

    filterFields = new ArrayList<>(2);
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      factory.compile("e"), DeduplicateAggregationDefinition.FilterFunction.MAX));
    filterFields.add(new DeduplicateAggregationDefinition.FilterExpression(
      factory.compile("f"), DeduplicateAggregationDefinition.FilterFunction.MIN));

    fullDefinition = DeduplicateAggregationDefinition.builder()
      .select(selectFields)
      .dedupOn(dedupFields)
      .filterDuplicatesBy(filterFields)
      .build();
    onlyDedupFieldsDefinition = DeduplicateAggregationDefinition.builder()
      .select(selectFields)
      .dedupOn(dedupFields)
      .build();

    helper = new BigQueryDeduplicateSQLBuilder(fullDefinition, "select * from tbl", "ds", "the_row_number");
  }

  @Test
  public void testGetQuery() {
    Assert.assertEquals("SELECT * EXCEPT(`the_row_number`) FROM ("
                          + "SELECT "
                          + "a AS alias_a , "
                          + "b AS alias_b , "
                          + "c AS c , "
                          + "d AS d , "
                          + "e AS e , "
                          + "f AS f , "
                          + "ROW_NUMBER() OVER ( PARTITION BY c , d , e ORDER BY e DESC NULLS LAST , f ASC NULLS LAST "
                          + ") AS `the_row_number` "
                          + "FROM ( select * from tbl ) AS ds"
                          + ") WHERE `the_row_number` = 1",
                        helper.getQuery());
  }

  @Test
  public void testGetInnerSelect() {
    Assert.assertEquals("SELECT "
                          + "a AS alias_a , "
                          + "b AS alias_b , "
                          + "c AS c , "
                          + "d AS d , "
                          + "e AS e , "
                          + "f AS f , "
                          + "ROW_NUMBER() OVER ( PARTITION BY c , d , e ORDER BY e DESC NULLS LAST , f ASC NULLS LAST "
                          + ") AS `the_row_number` "
                          + "FROM ( select * from tbl ) AS ds",
                        helper.getInnerSelect());
  }

  @Test
  public void testGetSelectedFields() {
    Assert.assertEquals(
      "a AS alias_a , "
        + "b AS alias_b , "
        + "c AS c , "
        + "d AS d , "
        + "e AS e , "
        + "f AS f , "
        + "ROW_NUMBER() OVER ( PARTITION BY c , d , e ORDER BY e DESC NULLS LAST , f ASC NULLS LAST ) AS" +
        " `the_row_number`",
      helper.getSelectedFields(fullDefinition));
  }

  @Test
  public void testGetRowNumColumn() {
    Assert.assertEquals("ROW_NUMBER() OVER ( PARTITION BY c , d , e ORDER BY e DESC NULLS LAST , " +
                          "f ASC NULLS LAST ) AS `the_row_number`",
                        helper.getRowNumColumn(fullDefinition));
  }


  @Test
  public void testGetRowNumColumnWithoutOrderFields() {
    Assert.assertEquals("ROW_NUMBER() OVER ( PARTITION BY c , d , e ) AS `the_row_number`",
                        helper.getRowNumColumn(onlyDedupFieldsDefinition));
  }

  @Test
  public void testGetPartitionByFields() {
    Assert.assertEquals("c , d , e", helper.getPartitionByFields(dedupFields));
  }

  @Test
  public void testGetOrderByFields() {
    Assert.assertEquals("e DESC NULLS LAST , f ASC NULLS LAST", helper.getOrderByFields(filterFields));
  }

  @Test
  public void testGetOrderByField() {
    DeduplicateAggregationDefinition.FilterExpression maxField1 =
      new DeduplicateAggregationDefinition.FilterExpression(factory.compile("field1"),
                                                            DeduplicateAggregationDefinition.FilterFunction.MAX);
    DeduplicateAggregationDefinition.FilterExpression minField2 =
      new DeduplicateAggregationDefinition.FilterExpression(factory.compile("field2"),
                                                            DeduplicateAggregationDefinition.FilterFunction.MIN);
    DeduplicateAggregationDefinition.FilterExpression minField3 =
      new DeduplicateAggregationDefinition
        .FilterExpression(factory.compile("field3"),
                          DeduplicateAggregationDefinition.FilterFunction.ANY_NULLS_FIRST);
    DeduplicateAggregationDefinition.FilterExpression minField4 =
      new DeduplicateAggregationDefinition
        .FilterExpression(factory.compile("field4"),
                          DeduplicateAggregationDefinition.FilterFunction.ANY_NULLS_LAST);

    Assert.assertEquals("field1 DESC NULLS LAST", helper.getOrderByField(maxField1));
    Assert.assertEquals("field2 ASC NULLS LAST", helper.getOrderByField(minField2));
    Assert.assertEquals("IF(field3 IS NULL , 0 , 1) ASC", helper.getOrderByField(minField3));
    Assert.assertEquals("IF(field4 IS NULL , 0 , 1) DESC", helper.getOrderByField(minField4));
  }


}
