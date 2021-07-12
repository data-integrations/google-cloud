/*
 * Copyright © 2021 Cask Data, Inc.
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
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Helper class used to generate BigQuery SQL Statements for Joins.
 * <p>
 * Many methods in this class have proteced visibility for the purposes of testing.
 */
public class BigQuerySQLBuilder {

  private static final String SELECT = "SELECT ";
  private static final String FROM = " FROM ";
  private static final String SPACE = " ";
  private static final String JOIN = " JOIN ";
  private static final String AS = " AS ";
  private static final String ON = " ON ";
  private static final String EQ = " = ";
  private static final String IS_NOT_DISTINCT_FROM = " IS NOT DISTINCT FROM ";
  private static final String AND = " AND ";
  private static final String OR = " OR ";
  private static final String DOT = ".";
  private static final String COMMA = " , ";
  private static final String IS_NULL = " IS NULL";
  private static final String OPEN_GROUP = "(";
  private static final String CLOSE_GROUP = ")";

  private final JoinDefinition joinDefinition;
  private final StringBuilder builder;
  private final String project;
  private final String dataset;
  private final Map<String, String> stageToBQTableNameMap;
  private final Map<String, String> stageToFullTableNameMap;
  private final Map<String, String> stageToTableAliasMap;

  public BigQuerySQLBuilder(JoinDefinition joinDefinition,
                            String project,
                            String dataset,
                            Map<String, String> stageToBQTableNameMap) {
    this(joinDefinition,
         project,
         dataset,
         stageToBQTableNameMap,
         new HashMap<>(),
         new HashMap<>(),
         new StringBuilder());
  }

  @VisibleForTesting
  protected BigQuerySQLBuilder(JoinDefinition joinDefinition,
                               String project,
                               String dataset,
                               Map<String, String> stageToBQTableNameMap,
                               Map<String, String> stageToFullTableNameMap,
                               Map<String, String> stageToTableAliasMap,
                               StringBuilder builder) {
    this.joinDefinition = joinDefinition;
    this.builder = builder;
    this.project = project;
    this.dataset = dataset;
    this.stageToBQTableNameMap = stageToBQTableNameMap;
    this.stageToFullTableNameMap = stageToFullTableNameMap;
    this.stageToTableAliasMap = stageToTableAliasMap;
  }

  public String getQuery() {
    if (joinDefinition.getCondition().getOp() == JoinCondition.Op.KEY_EQUALITY) {
      return getFieldEqualityQuery();
    } else {
      return getExpressionQuery();
    }
  }

  private String getFieldEqualityQuery() {
    // Build aliases for all tables.
    addTableNamesAndAliasesForJoinDefinition();

    appendSelectStatement();
    appendFieldEqualityClause();

    return builder.toString();
  }

  private String getExpressionQuery() {
    JoinCondition.OnExpression onExpression = (JoinCondition.OnExpression) joinDefinition.getCondition();
    // Build aliases for all tables.
    addTableNamesAndAliasesForJoinDefinition(onExpression.getDatasetAliases());

    appendSelectStatement();
    appendOnExpressionClause();

    return builder.toString();
  }

  /**
   * Appends Select columns and From statement
   * <p>
   * SELECT <fields> FROM ...
   */
  private void appendSelectStatement() {
    builder.append(SELECT).append(getSelectedFields()).append(FROM);
  }

  @VisibleForTesting
  protected String getSelectedFields() {
    return joinDefinition.getSelectedFields().stream()
      .map(this::buildSelectedField)
      .collect(Collectors.joining(COMMA));
  }

  @VisibleForTesting
  protected String buildSelectedField(JoinField joinField) {
    StringBuilder builder = new StringBuilder();

    builder.append(getTableAlias(joinField.getStageName()));
    builder.append(DOT);
    builder.append(joinField.getFieldName());

    if (joinField.getAlias() != null) {
      builder.append(AS);
      builder.append(quoteAlias(joinField.getAlias()));
    }

    return builder.toString();
  }

  private void appendFieldEqualityClause() {
    List<JoinStage> stages = joinDefinition.getStages();

    Map<String, JoinKey> stageNameToJoinKeyMap = new HashMap<>();
    for (JoinKey joinKey : ((JoinCondition.OnKeys) joinDefinition.getCondition()).getKeys()) {
      stageNameToJoinKeyMap.put(joinKey.getStageName(), joinKey);
    }
    boolean joinOnNullKeys = ((JoinCondition.OnKeys) joinDefinition.getCondition()).isNullSafe();

    appendFieldEqualityClause(stages, stageNameToJoinKeyMap, joinOnNullKeys);
  }

  @VisibleForTesting
  protected void appendFieldEqualityClause(List<JoinStage> stages,
                                           Map<String, JoinKey> stageNameToJoinKeyMap,
                                           boolean joinOnNullKeys) {
    JoinStage prev = null;

    for (JoinStage curr : stages) {
      if (prev == null) {
        appendFullTableNameAndAlias(curr.getStageName());
      } else {
        builder.append(SPACE);
        appendJoinOnKeyOperation(prev, curr, stageNameToJoinKeyMap, joinOnNullKeys);
      }

      prev = curr;
    }
  }

  private void appendOnExpressionClause() {
    JoinCondition.OnExpression onExpression = (JoinCondition.OnExpression) joinDefinition.getCondition();

    JoinStage left = joinDefinition.getStages().get(0);
    JoinStage right = joinDefinition.getStages().get(1);

    // Append Join Statement for these 2 stages
    // ...<left_table> <join_type> JOIN <left_table> ON ...
    appendFullTableNameAndAlias(left.getStageName());
    builder.append(SPACE);
    appendJoinStatement(left, right);
    builder.append(onExpression.getExpression());
  }

  /**
   * Appends join on key operation
   *
   * @param left                  left stage in the join
   * @param right                 right stage in the join
   * @param stageNameToJoinKeyMap Map containing all stage names and join keys
   * @param joinOnNullKeys        whether the join should include null kets
   */
  @VisibleForTesting
  protected void appendJoinOnKeyOperation(JoinStage left,
                                          JoinStage right,
                                          Map<String, JoinKey> stageNameToJoinKeyMap,
                                          boolean joinOnNullKeys) {
    // Append Join Statement for these 2 stages
    appendJoinStatement(left, right);

    String leftAlias = getTableAlias(left.getStageName());
    String rightAlias = getTableAlias(right.getStageName());

    JoinKey leftKey = stageNameToJoinKeyMap.get(left.getStageName());
    JoinKey rightKey = stageNameToJoinKeyMap.get(right.getStageName());

    // Append Join on key conditions
    appendJoinOnKeyClause(leftAlias, leftKey, rightAlias, rightKey, joinOnNullKeys);
  }

  /**
   * Appends join statement between 2 tables
   * <p>
   * ... [JOIN_TYPE] JOIN right ON ...
   *
   * @param left  left stage in the join
   * @param right right stage in the join
   */
  private void appendJoinStatement(JoinStage left,
                                   JoinStage right) {
    String joinType;

    if (left.isRequired() && right.isRequired()) {
      joinType = "INNER";
    } else if (left.isRequired() && !right.isRequired()) {
      joinType = "LEFT OUTER";
    } else if (!left.isRequired() && right.isRequired()) {
      joinType = "RIGHT OUTER";
    } else {
      joinType = "FULL OUTER";
    }

    // ... <join_type> JOIN <right_table> ON ...
    builder.append(joinType);
    builder.append(JOIN);
    appendFullTableNameAndAlias(right.getStageName());
    builder.append(ON);
  }

  /**
   * Appends a table name and alias for a given stage.
   * <p>
   * ...`project.dataset.bqtable` AS `somealias`...
   *
   * @param stageName name of the stage to use.
   */
  private void appendFullTableNameAndAlias(String stageName) {
    builder.append(getFullTableName(stageName)).append(AS).append(getTableAlias(stageName));
  }

  /**
   * Append join on key statement
   * <p>
   * When not joining on null keys, the result is:
   * ... left.l1 = right.r1 [AND left.l2 = right.r2 ...]
   * <p>
   * When  joining on null keys, the result is:
   * ... (left.l1 = right.r1 OR (left.l1 IS NULL AND right.r1 IS NULL))
   * [AND (left.l2 = right.r2 OR (left.l2 IS NULL AND right.r2 IS NULL))  ...]
   *
   * @param leftAlias
   * @param leftKey
   * @param rightAlias
   * @param rightKey
   * @param joinOnNullKeys
   */
  @VisibleForTesting
  protected void appendJoinOnKeyClause(String leftAlias,
                                       JoinKey leftKey,
                                       String rightAlias,
                                       JoinKey rightKey,
                                       boolean joinOnNullKeys) {
    // ... ON [left.l1 = right.r1]
    appendEquals(leftAlias, leftKey.getFields().get(0), rightAlias, rightKey.getFields().get(0), joinOnNullKeys);

    for (int i = 1; i < leftKey.getFields().size(); i++) {
      // ... [AND left.rN = right.rN]
      builder.append(AND);
      appendEquals(leftAlias, leftKey.getFields().get(i), rightAlias, rightKey.getFields().get(i), joinOnNullKeys);
    }
  }

  /**
   * Appends Equality clause
   * <p>
   * When not joining on null keys, the result is:
   * ... left.l1 = right.r1 ...
   * <p>
   * When joining on null keys, the result is:
   * ... (left.l1 = right.r1 OR (left.l1 IS NULL AND right.r1 IS NULL)) ...
   *
   * @param leftTable      Alias for the left table
   * @param leftField      Alias for the left field
   * @param rightTable     Alias for the right table
   * @param rightField     Alias for the right field
   * @param joinOnNullKeys if null kets should be included
   */
  @VisibleForTesting
  protected void appendEquals(String leftTable,
                              String leftField,
                              String rightTable,
                              String rightField,
                              boolean joinOnNullKeys) {
    builder.append(leftTable).append(DOT).append(leftField);

    if (joinOnNullKeys) {
      // ...table1.column1 IS NOT DISTINCT FROM table2.column2...
      builder.append(IS_NOT_DISTINCT_FROM);
    } else {
      // ...table1.column1 = table2.column2...
      builder.append(EQ);
    }

    builder.append(rightTable).append(DOT).append(rightField);
  }

  @VisibleForTesting
  protected String getBQTableName(String stageName) {
    String result = stageToBQTableNameMap.get(stageName);

    if (result == null) {
      throw new SQLEngineException(String.format("Unable to determine BQ table name for stage '%s'", stageName));
    }

    return result;
  }

  private void addTableNamesAndAliasesForJoinDefinition() {
    addTableNamesAndAliasesForJoinDefinition(Collections.emptyMap());
  }

  private void addTableNamesAndAliasesForJoinDefinition(Map<String, String> aliasOverrides) {
    for (JoinStage stage : joinDefinition.getStages()) {
      String stageName = stage.getStageName();

      if (!stageToFullTableNameMap.containsKey(stageName)) {
        addFullTableName(stageName);
      }

      if (!stageToTableAliasMap.containsKey(stageName)) {
        addTableAlias(stageName, aliasOverrides.getOrDefault(stageName, stageName));
      }
    }
  }

  /**
   * Aad the full table name for this table
   *
   * @param stageName
   */
  @VisibleForTesting
  protected void addFullTableName(String stageName) {
    String bqTableName = getBQTableName(stageName);
    stageToFullTableNameMap.put(stageName,
                                String.format("`%s.%s.%s`", project, dataset, bqTableName));
  }

  private void addTableAlias(String stageName, String alias) {
    stageToTableAliasMap.put(stageName, quoteAlias(alias));
  }

  /**
   * Add backticks to quote aliases for a given table.
   * <p>
   * This ensures aliases are escaped and support full unicode characters.
   *
   * @param alias alias to escape
   * @return quoted alias
   */
  @VisibleForTesting
  protected static String quoteAlias(String alias) {
    return String.format("`%s`", alias);
  }

  /**
   * Get fully qualified (quoted) table name for a supplied stage.
   */
  @VisibleForTesting
  protected String getFullTableName(String stageName) {
    String fullTableName = stageToFullTableNameMap.get(stageName);

    if (fullTableName == null) {
      throw new SQLEngineException(String.format("Unable to determine full table name for stage '%s'", stageName));
    }

    return fullTableName;
  }

  /**
   * Get table alias for a supplied stage.
   */
  private String getTableAlias(String stageName) {
    String tableAlias = stageToTableAliasMap.get(stageName);

    if (tableAlias == null) {
      throw new SQLEngineException(String.format("Unable to determine table alias for stage '%s'", stageName));
    }

    return tableAlias;
  }

}
