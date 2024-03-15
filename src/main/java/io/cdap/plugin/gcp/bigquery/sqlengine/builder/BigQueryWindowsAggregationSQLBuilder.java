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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.etl.api.aggregation.WindowAggregationDefinition;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.plugin.gcp.bigquery.relational.SQLExpression;
import io.cdap.plugin.gcp.bigquery.sqlengine.util.BigQuerySQLEngineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class used to generate BigQuery SQL Statements for Select statements.
 */
public class BigQueryWindowsAggregationSQLBuilder extends BigQueryBaseSQLBuilder {
  private static final String ROW_NUM_PREFIX = "rn_";
  private final WindowAggregationDefinition windowAggregationDefinition;
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryWindowsAggregationSQLBuilder.class);
  private final String source;
  private final String sourceAlias;
  private final String rowNumColumnAlias;
  private final StringBuilder builder;

  public BigQueryWindowsAggregationSQLBuilder(WindowAggregationDefinition windowAggregationDefinition,
                                              String sourceExpression,
                                              String sourceAlias) {
    this(windowAggregationDefinition,
         sourceExpression,
         sourceAlias,
         ROW_NUM_PREFIX + BigQuerySQLEngineUtils.newIdentifier());

  }

  protected BigQueryWindowsAggregationSQLBuilder(WindowAggregationDefinition windowAggregationDefinition,
                                                 String source,
                                                 String sourceAlias,
                                                 String rowNumColumnAlias) {
    this.windowAggregationDefinition = windowAggregationDefinition;
    this.source = source;
    this.sourceAlias = sourceAlias;
    // This is the alias used to store the row number value. Format is "rn_<uuid>"
    this.rowNumColumnAlias = rowNumColumnAlias;
    this.builder = new StringBuilder();
  }

  @VisibleForTesting
  protected String getSelectedFields(Map<String, Expression> selectedFields) {
    List<Expression> e = new ArrayList<>(selectedFields.values());
    return getPartitionFields(e);
  }

  @Override
  public String getQuery() {
    String overClause = getOverClause();
    String aggregateFields = getAggregateFields(overClause);
    /*The final query looks like SELECT <fields>, aggregate fields over (partition fields order fields windowFrameDef)
    FROM ( <source> ) AS sourceAlias*/
    String query = SELECT + getSelectedFields(windowAggregationDefinition.getSelectExpressions()) +
      COMMA + aggregateFields + SPACE + FROM + OPEN_GROUP + SPACE + source + SPACE + CLOSE_GROUP + SPACE + AS +
      sourceAlias;
    LOG.debug("Query is " + query);
    return query;
  }

  @VisibleForTesting
  protected String getAggregateFields(String overClause) {
    Map<String, Expression> aggregateExpressions = windowAggregationDefinition.getAggregateExpressions();

    return aggregateExpressions.keySet().stream().map(s -> {
      SQLExpression e = (SQLExpression) aggregateExpressions.get(s);
      return e.extract() + SPACE + overClause + AS + s;
    }).collect(Collectors.joining(COMMA));
  }

  /**
   * Gets over clause as a string. Over clause is always followed by aggregate function.
   *
   * @return over clause containing appended partition fields,order by fields, frame definition as string
   */
  private String getOverClause() {
    // OVER ( PARTITION BY <partitionFields> <orderFields> <windowFrameDef> )
    return OVER + OPEN_GROUP + SPACE + PARTITION_BY + SPACE +
      //Append partition fields
      getPartitionFields(windowAggregationDefinition.getPartitionExpressions()) + SPACE
      //Append order by fields
      + getOrderByFields(windowAggregationDefinition.getOrderByExpressions()) + SPACE +
      //Append window frame definition
      getWindowFrameDefinition(windowAggregationDefinition) + SPACE + CLOSE_GROUP;
  }

  private String getWindowFrameDefinition(WindowAggregationDefinition windowAggregationDefinition) {
    String def = EMPTY;
    if (windowAggregationDefinition.getWindowFrameType() == WindowAggregationDefinition.WindowFrameType.NONE) {
      return def;
    }
    if (windowAggregationDefinition.getWindowFrameType() == WindowAggregationDefinition.WindowFrameType.RANGE) {
      def = def + RANGE;
    } else if (windowAggregationDefinition.getWindowFrameType() == WindowAggregationDefinition.WindowFrameType.ROW) {
      def = def + ROWS;
    }
    def = def + BETWEEN;
    if (windowAggregationDefinition.getUnboundedPreceding()) {
      def = def + UNBOUNDED_PRECEDING;
    } else {
      int preceding = Integer.parseInt(windowAggregationDefinition.getPreceding());
      if (preceding == 0) {
        def = def + CURRENT_ROW;
      } else if (preceding < 0) {
        preceding = preceding * -1;
        def = def + preceding + PRECEDING;
      } else {
        def = def + preceding + FOLLOWING;
      }
    }
    def = def + AND;
    if (windowAggregationDefinition.getUnboundedFollowing()) {
      def = def + UNBOUNDED_FOLLOWING;
    } else {
      int following = Integer.parseInt(windowAggregationDefinition.getFollowing());
      if (following == 0) {
        def = def + CURRENT_ROW;
      } else if (following < 0) {
        following = following * -1;
        def = def + following + PRECEDING;
      } else {
        def = def + following + FOLLOWING;
      }
    }
    return def;
  }

  @VisibleForTesting
  public String getPartitionFields(List<Expression> partitionFields) {
    return getExpressionSQLStream(partitionFields).collect(Collectors.joining(COMMA));
  }

  @VisibleForTesting
  public String getOrderByFields(List<WindowAggregationDefinition.OrderByExpression> orderFields) {
    String order = EMPTY;
    for (WindowAggregationDefinition.OrderByExpression field : orderFields) {
      String type = field.getOrderBy().equals(WindowAggregationDefinition.OrderBy.ASCENDING) ? ORDER_ASC : ORDER_DESC;
      SQLExpression e = (SQLExpression) field.getExpression();

      if (EMPTY.equals(order)) {
        order = ORDER_BY + SPACE + e.extract() + SPACE + type;
      } else {
        order = order + COMMA + e.extract() + SPACE + type;
      }
    }
    return order;
  }
}
