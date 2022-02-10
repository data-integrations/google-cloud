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
import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.plugin.gcp.bigquery.relational.SQLExpression;
import io.cdap.plugin.gcp.bigquery.sqlengine.util.BigQuerySQLEngineUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class used to generate BigQuery SQL Statements for Select statements.
 */
public class BigQueryDeduplicateSQLBuilder extends BigQueryBaseSQLBuilder {
  private static final String ROW_NUM_PREFIX = "rn_";

  private final DeduplicateAggregationDefinition deduplicationDefinition;
  private final String source;
  private final String sourceAlias;
  private final String rowNumColumnAlias;

  public BigQueryDeduplicateSQLBuilder(DeduplicateAggregationDefinition deduplicationDefinition,
                                       String source,
                                       String sourceAlias) {
    this(deduplicationDefinition,
         source,
         sourceAlias,
         ROW_NUM_PREFIX + BigQuerySQLEngineUtils.newIdentifier());
  }

  @VisibleForTesting
  protected BigQueryDeduplicateSQLBuilder(DeduplicateAggregationDefinition deduplicationDefinition,
                                          String source,
                                          String sourceAlias,
                                          String rowNumColumnAlias) {
    this.deduplicationDefinition = deduplicationDefinition;
    this.source = source;
    this.sourceAlias = sourceAlias;
    // This is the alias used to store the row number value. Format is "rn_<uuid>"
    this.rowNumColumnAlias = rowNumColumnAlias;
  }

  public String getQuery() {
    return String.format(SELECT_DEDUPLICATE_STATEMENT,
                         rowNumColumnAlias,
                         getInnerSelect(),
                         rowNumColumnAlias);
  }

  /**
   * Get select statement for the underlying relation which includes a row number column used to deduplicate
   * @return select statement that can be used to enumerate duplicate rows
   */
  @VisibleForTesting
  protected String getInnerSelect() {
    return SELECT + getSelectedFields(deduplicationDefinition) +
      FROM + OPEN_GROUP + SPACE + source + SPACE + CLOSE_GROUP + AS + QUOTE + sourceAlias + QUOTE;
  }

  /**
   * Gets selected fields as a string. This also includes a field used for assigning row numbers.
   *
   * @return selected fields separated by commas
   */
  @VisibleForTesting
  protected String getSelectedFields(DeduplicateAggregationDefinition def) {
    Set<String> columns = def.getSelectExpressions().entrySet()
      .stream()
      .map(e -> ((SQLExpression) e.getValue()).getExpression() + AS + QUOTE + e.getKey() + QUOTE)
      .collect(Collectors.toCollection(LinkedHashSet::new));
    columns.add(getRowNumColumn(def));
    return String.join(COMMA, columns);
  }

  /**
   * Build statement to generate a row number based on the supplied deduplication definition.
   * @param def deduplication definition
   * @return statement used to assign row numbers to output columns.
   */
  @VisibleForTesting
  protected String getRowNumColumn(DeduplicateAggregationDefinition def) {
    String partitionByFields = getPartitionByFields(def.getGroupByExpressions());
    String orderByFields = getOrderByFields(def.getFilterExpressions());
    return String.format(ROW_NUMBER_PARTITION_COLUMN,
                         partitionByFields,
                         orderByFields,
                         rowNumColumnAlias);
  }

  /**
   * Get fields used for partitioning
   * @param partitionByExpressions expressions used for partitioning
   * @return expressions separated by a comma.
   */
  @VisibleForTesting
  protected String getPartitionByFields(List<Expression> partitionByExpressions) {
    return partitionByExpressions
      .stream()
      .map(e -> ((SQLExpression) e).getExpression())
      .collect(Collectors.joining(COMMA));
  }

  /**
   * Get fields used for ordering.
   * @param orderByExpression expressions used for ordering
   * @return order by expressions separated by a comma.
   */
  @VisibleForTesting
  protected String getOrderByFields(List<DeduplicateAggregationDefinition.FilterExpression> orderByExpression) {
    return orderByExpression
      .stream()
      .map(this::getOrderByField)
      .collect(Collectors.joining(COMMA));
  }

  /**
   * Buinds Order By expression based on a filter expression.
   * @param filterExpression supplied expression
   * @return Order by SQL expression
   */
  protected String getOrderByField(DeduplicateAggregationDefinition.FilterExpression filterExpression) {
    String order;

    // MAX of a value means ORDER DESCENDING and selecting the first result.
    // MIN of a value means ORDER ASCENDING and selecting the first result.
    if (filterExpression.getFilterFunction() == DeduplicateAggregationDefinition.FilterFunction.MAX) {
      order = ORDER_DESC;
    } else {
      order = ORDER_ASC;
    }

    // some_field ASC/DESC
    return ((SQLExpression) filterExpression.getExpression()).getExpression() + SPACE + order;
  }

}
