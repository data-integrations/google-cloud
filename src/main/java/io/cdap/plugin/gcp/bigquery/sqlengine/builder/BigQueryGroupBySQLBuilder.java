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
import io.cdap.cdap.etl.api.aggregation.AggregationDefinition;
import io.cdap.cdap.etl.api.relational.Expression;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class used to generate BigQuery SQL Statements for Select statements.
 */
public class BigQueryGroupBySQLBuilder extends BigQueryBaseSQLBuilder {
  private final AggregationDefinition aggregationDefinition;
  private final String sourceExpression;
  private final String sourceAlias;
  private final StringBuilder builder;

  public BigQueryGroupBySQLBuilder(AggregationDefinition aggregationDefinition,
                                   String sourceExpression,
                                   String sourceAlias) {
    this.aggregationDefinition = aggregationDefinition;
    this.sourceExpression = sourceExpression;
    this.sourceAlias = sourceAlias;
    this.builder = new StringBuilder();
  }

  public String getQuery() {
    // SELECT ...
    builder.append(SELECT).append(getSelectedFields(aggregationDefinition.getSelectExpressions()));

    // FROM (select ... from ...) AS `source_alias`
    builder.append(FROM);
    builder.append(OPEN_GROUP).append(SPACE).append(sourceExpression).append(SPACE).append(CLOSE_GROUP);
    builder.append(AS).append(QUOTE).append(sourceAlias).append(QUOTE);

    // GROUP BY
    builder.append(GROUP_BY).append(getGroupByFields(aggregationDefinition.getGroupByExpressions()));

    return builder.toString();
  }

  /**
   * Gets selected fields as a string. This also includes a field used for assigning row numbers.
   * @param selectedFields map containing "alias" -> "field expression"
   * @return selected fields separated by commas
   */
  @VisibleForTesting
  protected String getSelectedFields(Map<String, Expression> selectedFields) {
    return getSelectColumnsStream(selectedFields)
      .collect(Collectors.joining(COMMA));
  }

  /**
   * Get fields used for partitioning
   * @param groupByExpressions expressions used for grouping
   * @return expressions separated by a comma.
   */
  @VisibleForTesting
  protected String getGroupByFields(List<Expression> groupByExpressions) {
    return getExpressionSQLStream(groupByExpressions)
      .collect(Collectors.joining(COMMA));
  }


}
