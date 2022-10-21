package io.cdap.plugin.gcp.bigquery.sqlengine.builder;

import io.cdap.cdap.etl.api.aggregation.WindowAggregationDefinition;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.plugin.gcp.bigquery.relational.SQLExpressionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BigQueryWindowsAggregationSQLBuilderTest {
  private BigQueryWindowsAggregationSQLBuilder helper;
  private SQLExpressionFactory factory;
  private Map<String, Expression> selectFields;
  private List<Expression> partitionFields;
  private Map<String, Expression> aggregationFields;
  private List<WindowAggregationDefinition.OrderByExpression> orderFields;
  private WindowAggregationDefinition fullDefinition;
  private WindowAggregationDefinition.WindowFrameType windowFrameType;

  @Before
  public void setUp() {
    factory = new SQLExpressionFactory();

    // Build aggregation definition
    aggregationFields = new HashMap<>();
    aggregationFields.put("f", factory.compile("first_value(f)"));
    aggregationFields.put("g", factory.compile("last_value(g)"));
    selectFields = new LinkedHashMap<>();
    selectFields.put("alias_a", factory.compile("a"));
    selectFields.put("alias_b", factory.compile("b"));
    selectFields.put("c", factory.compile("c"));
    selectFields.put("d", factory.compile("d"));
    selectFields.put("e", factory.compile("e"));
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
      .aggregate(aggregationFields).orderBy(orderFields).windowFrameType(windowFrameType).unboundedFollowing(true)
      .unboundedPreceding(true).build();
    helper = new BigQueryWindowsAggregationSQLBuilder(fullDefinition, "select * from tbl", "ds",
                                                      "the_row_number");
  }

  @Test
  public void testGetQuery() {
    Assert.assertEquals("SELECT a , b , c , d , e , first_value(f) OVER( PARTITION BY  a , b ORDER BY  c " +
                          "ASC , d DESC  ) AS f , last_value(g) OVER( PARTITION BY  a , b ORDER BY  c ASC , " +
                          "d DESC  ) AS g  FROM ( select * from tbl )  AS ds", helper.getQuery());
  }

  @Test
  public void testGetQueryWithBoundedCondition() {
    windowFrameType = WindowAggregationDefinition.WindowFrameType.ROW;
    fullDefinition = WindowAggregationDefinition.builder().select(selectFields).partition(partitionFields)
      .aggregate(aggregationFields).orderBy(orderFields).windowFrameType(windowFrameType).unboundedFollowing(false)
      .unboundedPreceding(false).preceding("1").following("1").build();
    helper = new BigQueryWindowsAggregationSQLBuilder(fullDefinition, "select * from tbl", "ds",
                                                      "the_row_number");
    Assert.assertEquals("SELECT a , b , c , d , e , first_value(f) OVER( PARTITION BY  a , b ORDER BY  c " +
       "ASC , d DESC ROWS BETWEEN 1 PRECEDING  AND 1 FOLLOWING  ) AS f , last_value(g) OVER( PARTITION BY  a , b ORDER"
       + " BY  c ASC , d DESC ROWS BETWEEN 1 PRECEDING  AND 1 FOLLOWING  ) AS g  FROM ( select * from tbl )  AS ds",
       helper.getQuery());
  }

  @Test
  public void testGetSelectedFields() {
    Assert.assertEquals("a , b , c , d , e",
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

  @Test
  public void testGetAggregateFields() {
    Assert.assertEquals("first_value(f) over AS f , last_value(g) over AS g",
                        helper.getAggregateFields("over"));
  }
}
