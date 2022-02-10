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

import java.util.Map;

/**
 * Helper class used to generate BigQuery SQL Statements for nested Select statements.
 * <p>
 * Nested statements are created when a select statement is built on top of a previous select statement.
 * For example: SELECT a,b FROM (select a,b,c from other_table) AS alias
 */
public class BigQueryNestedSelectSQLBuilder extends BigQuerySelectSQLBuilder {

  public BigQueryNestedSelectSQLBuilder(Map<String, String> columns,
                                        String source,
                                        String sourceAlias,
                                        String filter) {
    super(columns, source, sourceAlias, filter);
  }

  /**
   * From expression for nested queries requires wrapping the underlying query in parentheses.
   *
   * @return expression used when building the FROM statement
   */
  @Override
  protected String getFromSource() {
    // FROM (some_sql_statement) ...
    return OPEN_GROUP + sourceTable + CLOSE_GROUP;
  }

}
