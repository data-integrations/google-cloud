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
import io.cdap.cdap.etl.api.relational.Expression;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class used to generate BigQuery SQL Statements for Select statements.
 */
public class BigQuerySelectSQLBuilder extends BigQueryBaseSQLBuilder {

  protected final Map<String, Expression> columns;
  protected final String sourceTable;
  protected final String sourceAlias;
  protected final String filter;
  protected final StringBuilder builder;

  public BigQuerySelectSQLBuilder(Map<String, Expression> columns,
                                  String sourceTable,
                                  String sourceAlias,
                                  String filter) {
    this.columns = columns;
    this.sourceTable = sourceTable;
    this.sourceAlias = sourceAlias;
    this.filter = filter;
    this.builder = new StringBuilder();
  }

  public String getQuery() {
    // SELECT ...
    builder.append(SELECT).append(getSelectedFields());
    // FROM some_from_table AS `some_alias`
    builder.append(FROM).append(getFromSource()).append(AS).append(QUOTE).append(sourceAlias).append(QUOTE);

    if (filter != null) {
      // WHERE ...
      builder.append(WHERE).append(filter);
    }

    return builder.toString();
  }

  /**
   * For simple select statements, the underlying table name is used as is.
   * SELECT ... FROM table ...
   *
   * @return expression used when building the FROM statement
   */
  protected String getFromSource() {
    return sourceTable;
  }

  /**
   * Gets selected fields as a string.
   *
   * @return selected fields separated by commas
   */
  @VisibleForTesting
  protected String getSelectedFields() {
    return getSelectColumnsStream(columns)
      .collect(Collectors.joining(COMMA));
  }

}
