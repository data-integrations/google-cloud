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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class used to generate BigQuery SQL Statements
 */
public class BigQuerySQLBuilder {
  private final StringBuilder builder;
  private final String project;
  private final String dataset;
  private final Map<String, String> stageToBQTableNameMap;
  private final Map<String, String> stageToFullTableNameMap;
  private final Map<String, String> stageToTableAliasMap;

  public BigQuerySQLBuilder(String project, String dataset, Map<String, String> stageToBQTableNameMap) {
    this(new StringBuilder(), project, dataset, stageToBQTableNameMap, new HashMap<>(), new HashMap<>());
  }

  @VisibleForTesting
  protected BigQuerySQLBuilder(StringBuilder builder,
                               String project,
                               String dataset,
                               Map<String, String> stageToBQTableNameMap,
                               Map<String, String> stageToFullTableNameMap,
                               Map<String, String> stageToTableAliasMap) {
    this.builder = builder;
    this.project = project;
    this.dataset = dataset;
    this.stageToBQTableNameMap = stageToBQTableNameMap;
    this.stageToFullTableNameMap = stageToFullTableNameMap;
    this.stageToTableAliasMap = stageToTableAliasMap;
  }

  public String getFieldEqualityQuery(JoinDefinition joinDefinition) {
    // Build aliases for all tables.
    addTableNamesAndAliasesForJoinDefinition(joinDefinition);

    //Reset builder
    builder.setLength(0);

    builder.append("SELECT ").append(getSelectedFields(joinDefinition));
    builder.append(" FROM ");
    appendFieldEqualityClause(joinDefinition);

    return builder.toString();
  }

  protected String getSelectedFields(JoinDefinition joinDefinition) {
    return joinDefinition.getSelectedFields().stream()
      .map(this::buildSelectedField)
      .collect(Collectors.joining(" , "));
  }

  protected String buildSelectedField(JoinField joinField) {
    StringBuilder builder = new StringBuilder();

    builder.append(getTableAlias(joinField.getStageName()));
    builder.append(".");
    builder.append(joinField.getFieldName());

    if (joinField.getAlias() != null) {
      builder.append(" AS ");
      builder.append(joinField.getAlias());
    }

    return builder.toString();
  }

  protected void appendFieldEqualityClause(JoinDefinition joinDefinition) {
    List<JoinStage> stages = joinDefinition.getStages();

    Map<String, JoinKey> stageNameToJoinKeyMap = new HashMap<>();
    for (JoinKey joinKey : ((JoinCondition.OnKeys) joinDefinition.getCondition()).getKeys()) {
      stageNameToJoinKeyMap.put(joinKey.getStageName(), joinKey);
    }
    boolean joinOnNullKeys = ((JoinCondition.OnKeys) joinDefinition.getCondition()).isNullSafe();

    appendFieldEqualityClause(stages, stageNameToJoinKeyMap, joinOnNullKeys);
  }

  protected void appendFieldEqualityClause(List<JoinStage> stages,
                                           Map<String, JoinKey> stageNameToJoinKeyMap,
                                           boolean joinOnNullKeys) {
    JoinStage prev = null;

    for (JoinStage curr : stages) {
      if (prev == null) {
        String currStage = curr.getStageName();
        builder.append(getFullTableName(currStage)).append(" AS ").append(getTableAlias(currStage));
      } else {
        builder.append(" ");
        appendJoinOnKeyOperation(prev, curr, stageNameToJoinKeyMap, joinOnNullKeys);
      }

      prev = curr;
    }
  }

  protected void appendJoinOnKeyOperation(JoinStage left,
                                          JoinStage right,
                                          Map<String, JoinKey> stageNameToJoinKeyMap,
                                          boolean joinOnNullKeys) {
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

    String leftAlias = getTableAlias(left.getStageName());
    String rightAlias = getTableAlias(right.getStageName());
    String rightTable = getFullTableName(right.getStageName());

    // ... <join_type> JOIN <right_table> ON ...
    builder.append(joinType);
    builder.append(" JOIN ");
    builder.append(rightTable);
    builder.append(" AS ");
    builder.append(rightAlias);
    builder.append(" ON ");

    JoinKey leftKey = stageNameToJoinKeyMap.get(left.getStageName());
    JoinKey rightKey = stageNameToJoinKeyMap.get(right.getStageName());

    appendJoinOnKeyStatement(leftAlias, leftKey, rightAlias, rightKey, joinOnNullKeys);
  }

  protected void appendJoinOnKeyStatement(String leftAlias,
                                          JoinKey leftKey,
                                          String rightAlias,
                                          JoinKey rightKey,
                                          boolean joinOnNullKeys) {
    // ... ON [left.l1 = right.r1]
    appendEquals(leftAlias, leftKey.getFields().get(0), rightAlias, rightKey.getFields().get(0), joinOnNullKeys);

    for (int i = 1; i < leftKey.getFields().size(); i++) {
      // ... [AND left.rN = right.rN]
      builder.append(" AND ");
      appendEquals(leftAlias, leftKey.getFields().get(i), rightAlias, rightKey.getFields().get(i), joinOnNullKeys);
    }
  }

  protected void appendEquals(String leftAlias,
                              String leftField,
                              String rightAlias,
                              String rightField,
                              boolean joinOnNullKeys) {
    if (joinOnNullKeys) {
      builder.append("(");
    }

    builder.append(leftAlias).append(".").append(leftField);
    builder.append(" = ");
    builder.append(rightAlias).append(".").append(rightField);

    if (joinOnNullKeys) {
      builder.append(" OR (");
      builder.append(leftAlias).append(".").append(leftField).append(" IS NULL");
      builder.append(" AND ");
      builder.append(rightAlias).append(".").append(rightField).append(" IS NULL");
      builder.append(")").append(")");
    }
  }

  protected String getBQTableName(String stageName) {
    String result = stageToBQTableNameMap.get(stageName);

    if (result == null) {
      throw new SQLEngineException(String.format("Unable to determine BQ table name for stage '%s'", stageName));
    }

    return result;
  }

  protected void addTableNamesAndAliasesForJoinDefinition(JoinDefinition joinDefinition) {
    for (JoinStage stage : joinDefinition.getStages()) {
      String stageName = stage.getStageName();

      if (!stageToFullTableNameMap.containsKey(stageName)) {
        addFullTableName(stageName);
      }

      if (!stageToTableAliasMap.containsKey(stageName)) {
        addTableAlias(stageName);
      }
    }
  }

  protected void addFullTableName(String stageName) {
    String bqTableName = getBQTableName(stageName);
    stageToFullTableNameMap.put(stageName,
                                String.format("`%s.%s.%s`", project, dataset, bqTableName));
  }

  protected void addTableAlias(String stageName) {
    stageToTableAliasMap.put(stageName,
                             buildTableAlias(stageToTableAliasMap.size()));
  }

  /**
   * Get fully qualified (quoted) table name for a supplied stage.
   */
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
  protected String getTableAlias(String stageName) {
    String tableAlias = stageToTableAliasMap.get(stageName);

    if (tableAlias == null) {
      throw new SQLEngineException(String.format("Unable to determine table alias for stage '%s'", stageName));
    }

    return tableAlias;
  }

  /**
   * Builds a column alias based on an index.
   *
   * The resulting Alias follows the spreadsheet column naming convention:
   * 0 -> A
   * ...
   * 25 -> Z
   * 26 -> AA
   * 27 -> AB
   *
   * @param index the index to generate
   * @return the table alias for the current index.
   */
  protected String buildTableAlias(int index) {
    // Build all preceding characters in the alias, if needed.
    if (index / 26 > 0) {
      return buildTableAlias((index / 26) - 1) + (char) ('A' + index % 26);
    } else {
      return String.valueOf((char) ('A' + index));
    }
  }

}
