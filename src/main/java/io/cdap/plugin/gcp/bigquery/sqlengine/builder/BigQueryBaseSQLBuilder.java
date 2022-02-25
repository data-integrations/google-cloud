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

import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.plugin.gcp.bigquery.relational.SQLExpression;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Base class which defines convenience variables to be used then building SQL expressions
 */
public abstract class BigQueryBaseSQLBuilder {
  public static final String SELECT = "SELECT ";
  public static final String FROM = " FROM ";
  public static final String SPACE = " ";
  public static final String JOIN = " JOIN ";
  public static final String AS = " AS ";
  public static final String ON = " ON ";
  public static final String EQ = " = ";
  public static final String AND = " AND ";
  public static final String OR = " OR ";
  public static final String DOT = ".";
  public static final String COMMA = " , ";
  public static final String IS_NULL = " IS NULL";
  public static final String OPEN_GROUP = "(";
  public static final String CLOSE_GROUP = ")";
  public static final String WHERE = " WHERE ";
  public static final String GROUP_BY = " GROUP BY ";
  public static final String QUOTE = "`";
  public static final String ORDER_DESC = "DESC";
  public static final String ORDER_ASC = "ASC";
  public static final String SELECT_DEDUPLICATE_STATEMENT = "SELECT * EXCEPT(`%s`) FROM (%s) WHERE `%s` = 1";
  public static final String ROW_NUMBER_PARTITION_COLUMN =
    "ROW_NUMBER() OVER ( PARTITION BY %s ORDER BY %s ) AS `%s`";
  public static final String NULLS_LAST = "NULLS LAST";

  /**
   * Builds SQL statement
   */
  public abstract String getQuery();

  /**
   * Generates a {@link Stream} of {@link String} containing field selection expressions for the input columns.
   *
   * The key for this map is used as an alias and the expression value is used as the selected column/function.
   * For example,: "field_name AS `field_alias`".
   * @param selectFields Map of "alias" -> "field expression"
   * @return Stream containing select expressions
   */
  public Stream<String> getSelectColumnsStream(Map<String, Expression> selectFields) {
    return selectFields.entrySet()
      .stream()
      .map(e -> ((SQLExpression) e.getValue()).extract() + AS + e.getKey());
  }

  /**
   * Generates a {@link Stream} of {@link String} containing values enclosed by Expressions.
   *
   * @param expressions expressions
   * @return Stream containing extracted expressions as Strings
   */
  public Stream<String> getExpressionSQLStream(Collection<Expression> expressions) {
    return expressions
      .stream()
      .map(e -> ((SQLExpression) e).extract());
  }
}
