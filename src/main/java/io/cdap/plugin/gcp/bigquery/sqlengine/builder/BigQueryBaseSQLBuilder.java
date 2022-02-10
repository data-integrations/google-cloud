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

/**
 * Base class which defines convenience variables to be used then building SQL expressions
 */
public abstract class BigQueryBaseSQLBuilder {
  protected static final String SELECT = "SELECT ";
  protected static final String FROM = " FROM ";
  protected static final String SPACE = " ";
  protected static final String JOIN = " JOIN ";
  protected static final String AS = " AS ";
  protected static final String ON = " ON ";
  protected static final String EQ = " = ";
  protected static final String AND = " AND ";
  protected static final String OR = " OR ";
  protected static final String DOT = ".";
  protected static final String COMMA = " , ";
  protected static final String IS_NULL = " IS NULL";
  protected static final String OPEN_GROUP = "(";
  protected static final String CLOSE_GROUP = ")";
  protected static final String WHERE = " WHERE ";
  protected static final String QUOTE = "`";
  protected static final String ORDER_DESC = "DESC";
  protected static final String ORDER_ASC = "ASC";
  protected static final String SELECT_DEDUPLICATE_STATEMENT = "SELECT * EXCEPT(`%s`) FROM (%s) WHERE `%s` = 1";
  protected static final String ROW_NUMBER_PARTITION_COLUMN =
    "ROW_NUMBER() OVER ( PARTITION BY %s ORDER BY %s ) AS `%s`";

  /**
   * Builds SQL statement
   */
  public abstract String getQuery();
}
