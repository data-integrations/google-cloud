package io.cdap.plugin.gcp.bigquery.sqlengine.builder;

import io.cdap.cdap.etl.api.aggregation.WindowAggregationDefinition;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.plugin.gcp.bigquery.relational.SQLExpressionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BigQueryWindowsAggregationSQLBuilderTest {
  private BigQueryWindowsAggregationSQLBuilder helper;
  private SQLExpressionFactory factory;
  private Map<String, Expression> selectFields;
  private List<Expression> partitionFields;
  private List<WindowAggregationDefinition.OrderByExpression> orderFields;
  private WindowAggregationDefinition fullDefinition;
  private WindowAggregationDefinition.WindowFrameType windowFrameType;

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
    selectFields.put("f", factory.compile("first_value(f)"));
    partitionFields = new ArrayList<>();
    partitionFields.add(factory.compile("a"));
    partitionFields.add(factory.compile("b"));
    orderFields = new ArrayList<>();
    orderFields.add(new WindowAggregationDefinition.OrderByExpression(factory.compile("c"),
                                                                      WindowAggregationDefinition.OrderBy.ASCENDING));
    orderFields.add(new WindowAggregationDefinition.OrderByExpression(factory.compile("d"),
                                                                      WindowAggregationDefinition.OrderBy.DESCENDING));
    windowFrameType = WindowAggregationDefinition.WindowFrameType.NONE;
    fullDefinition = WindowAggregationDefinition.builder().select(selectFields).partition(partitionFields)
      .orderBy(orderFields).windowFrameType(windowFrameType).unboundedFollowing(true).unboundedPreceding(true)
        .build();
    helper = new BigQueryWindowsAggregationSQLBuilder(fullDefinition, "select * from tbl", "ds",
                                                      "the_row_number");
  }

  @Test
  public void testGetQuery() {
    Assert.assertEquals("SELECT a , b , c , d , e , first_value(f) OVER( PARTITION BY  a , b ORDER BY  c ASC ,"
      + " d DESC  ) FROM ( select * from tbl )  AS ds", helper.getQuery());
  }

  @Test
  public void testGetSelectedFields() {
    Assert.assertEquals("a , b , c , d , e , first_value(f)",
                        helper.getSelectedFields(fullDefinition.getSelectExpressions()));
  }

  @Test
  public void testGetPartitionByFields() {
    Assert.assertEquals("a , b", helper.getPartitionFields(partitionFields));
  }

  @Test
  public void testGetOrderByFields() {
    Assert.assertEquals("ORDER BY  c ASC , d DESC", helper.getOrderByFields(orderFields));
  }
}
