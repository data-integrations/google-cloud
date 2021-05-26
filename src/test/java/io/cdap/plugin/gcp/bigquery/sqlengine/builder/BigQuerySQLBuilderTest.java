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

import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class BigQuerySQLBuilderTest {
  StringBuilder builder;
  BigQuerySQLBuilder helper;
  Map<String, String> stageToBQTableNameMap;
  Map<String, String> stageToFullTableNameMap;
  Map<String, String> stageToTableAliasMap;
  String project;
  String dataset;

  @Before
  public void setUp() {
    builder = new StringBuilder();
    stageToBQTableNameMap = new HashMap<>();
    stageToBQTableNameMap.put("leftStage", "bqLeft");
    stageToBQTableNameMap.put("rightStage", "bqRight");
    stageToBQTableNameMap.put("stage1", "bqTable1");
    stageToBQTableNameMap.put("stage2", "bqTable2");
    stageToBQTableNameMap.put("stage3", "bqTable3");
    stageToBQTableNameMap.put("stage4", "bqTable4");
    stageToBQTableNameMap.put("stage5", "bqTable5");
    stageToFullTableNameMap = new HashMap<>();
    stageToFullTableNameMap.put("leftStage", "`some.bq.left`");
    stageToFullTableNameMap.put("rightStage", "`some.bq.right`");
    stageToFullTableNameMap.put("stage1", "`bq.stage.1`");
    stageToFullTableNameMap.put("stage2", "`bq.stage.2`");
    stageToFullTableNameMap.put("stage3", "`bq.stage.3`");
    stageToFullTableNameMap.put("stage4", "`bq.stage.4`");
    stageToFullTableNameMap.put("stage5", "`bq.stage.5`");
    stageToTableAliasMap = new HashMap<>();
    stageToTableAliasMap.put("leftStage", "LEFT");
    stageToTableAliasMap.put("rightStage", "RIGHT");
    stageToTableAliasMap.put("stage1", "T1");
    stageToTableAliasMap.put("stage2", "T2");
    stageToTableAliasMap.put("stage3", "T3");
    stageToTableAliasMap.put("stage4", "T4");
    stageToTableAliasMap.put("stage5", "T5");
    project = "project";
    dataset = "dataset";

    helper = spy(new BigQuerySQLBuilder(builder, project, dataset,
                                        stageToBQTableNameMap, stageToFullTableNameMap, stageToTableAliasMap));
  }

  @Test
  public void testAppendFieldEqualityClause() {

    JoinStage stage1 = JoinStage.builder("stage1", null).build();
    // left join between stage1 and stage2
    JoinStage stage2 = JoinStage.builder("stage2", null).setRequired(false).build();
    // full outer join between stage2 and stage3
    JoinStage stage3 = JoinStage.builder("stage3", null).setRequired(false).build();
    // right join between stage3 and stage4
    JoinStage stage4 = JoinStage.builder("stage4", null).build();
    // inner join join between stage4 and stage5
    JoinStage stage5 = JoinStage.builder("stage5", null).build();

    Map<String, JoinKey> stageNameToJoinKeyMap = new HashMap<>();
    stageNameToJoinKeyMap.put("stage1", new JoinKey("stage1", Collections.singletonList("a")));
    stageNameToJoinKeyMap.put("stage2", new JoinKey("stage2", Collections.singletonList("b")));
    stageNameToJoinKeyMap.put("stage3", new JoinKey("stage3", Collections.singletonList("c")));
    stageNameToJoinKeyMap.put("stage4", new JoinKey("stage4", Collections.singletonList("d")));
    stageNameToJoinKeyMap.put("stage5", new JoinKey("stage5", Collections.singletonList("e")));
    List<JoinStage> stages = Arrays.asList(stage1, stage2, stage3, stage4, stage5);

    helper.appendFieldEqualityClause(stages, stageNameToJoinKeyMap, false);

    Assert.assertEquals("`bq.stage.1` AS T1"
                          + " LEFT OUTER JOIN `bq.stage.2` AS T2 ON T1.a = T2.b"
                          + " FULL OUTER JOIN `bq.stage.3` AS T3 ON T2.b = T3.c"
                          + " RIGHT OUTER JOIN `bq.stage.4` AS T4 ON T3.c = T4.d"
                          + " INNER JOIN `bq.stage.5` AS T5 ON T4.d = T5.e",
                        builder.toString());
  }

  @Test
  public void testAppendFieldEqualityClauseNullSafe() {

    JoinStage stage1 = JoinStage.builder("stage1", null).build();
    // left join between stage1 and stage2
    JoinStage stage2 = JoinStage.builder("stage2", null).setRequired(false).build();
    // full outer join between stage2 and stage3
    JoinStage stage3 = JoinStage.builder("stage3", null).setRequired(false).build();
    // right join between stage3 and stage4
    JoinStage stage4 = JoinStage.builder("stage4", null).build();
    // inner join join between stage4 and stage5
    JoinStage stage5 = JoinStage.builder("stage5", null).build();

    Map<String, JoinKey> stageNameToJoinKeyMap = new HashMap<>();
    stageNameToJoinKeyMap.put("stage1", new JoinKey("stage1", Collections.singletonList("a")));
    stageNameToJoinKeyMap.put("stage2", new JoinKey("stage2", Collections.singletonList("b")));
    stageNameToJoinKeyMap.put("stage3", new JoinKey("stage3", Collections.singletonList("c")));
    stageNameToJoinKeyMap.put("stage4", new JoinKey("stage4", Collections.singletonList("d")));
    stageNameToJoinKeyMap.put("stage5", new JoinKey("stage5", Collections.singletonList("e")));
    List<JoinStage> stages = Arrays.asList(stage1, stage2, stage3, stage4, stage5);

    helper.appendFieldEqualityClause(stages, stageNameToJoinKeyMap, true);

    Assert.assertEquals("`bq.stage.1` AS T1"
                          + " LEFT OUTER JOIN `bq.stage.2` AS T2 ON (T1.a = T2.b OR (T1.a IS NULL AND T2.b IS NULL))"
                          + " FULL OUTER JOIN `bq.stage.3` AS T3 ON (T2.b = T3.c OR (T2.b IS NULL AND T3.c IS NULL))"
                          + " RIGHT OUTER JOIN `bq.stage.4` AS T4 ON (T3.c = T4.d OR (T3.c IS NULL AND T4.d IS NULL))"
                          + " INNER JOIN `bq.stage.5` AS T5 ON (T4.d = T5.e OR (T4.d IS NULL AND T5.e IS NULL))",
                        builder.toString());
  }

  @Test
  public void testBuildJoinOperationInnerJoin() {
    Map<String, JoinKey> stringJoinKeyMap = new HashMap<>();
    configureJoinOperationTest(stringJoinKeyMap);

    JoinStage leftStage = JoinStage.builder("leftStage", null).setRequired(true).build();
    JoinStage rightStage = JoinStage.builder("rightStage", null).setRequired(true).build();

    helper.appendJoinOnKeyOperation(leftStage, rightStage, stringJoinKeyMap, false);
    Assert.assertEquals("INNER JOIN `some.bq.right` AS RIGHT ON some join condition",
                        builder.toString());
  }

  @Test
  public void testBuildJoinOperationLeftJoin() {
    Map<String, JoinKey> stringJoinKeyMap = new HashMap<>();
    configureJoinOperationTest(stringJoinKeyMap);

    JoinStage leftStage = JoinStage.builder("leftStage", null).setRequired(true).build();
    JoinStage rightStage = JoinStage.builder("rightStage", null).setRequired(false).build();

    helper.appendJoinOnKeyOperation(leftStage, rightStage, stringJoinKeyMap, false);
    Assert.assertEquals("LEFT OUTER JOIN `some.bq.right` AS RIGHT ON some join condition",
                        builder.toString());
  }

  @Test
  public void testBuildJoinOperationRightJoin() {
    Map<String, JoinKey> stringJoinKeyMap = new HashMap<>();
    configureJoinOperationTest(stringJoinKeyMap);

    JoinStage leftStage = JoinStage.builder("leftStage", null).setRequired(false).build();
    JoinStage rightStage = JoinStage.builder("rightStage", null).setRequired(true).build();

    helper.appendJoinOnKeyOperation(leftStage, rightStage, stringJoinKeyMap, false);
    Assert.assertEquals("RIGHT OUTER JOIN `some.bq.right` AS RIGHT ON some join condition",
                        builder.toString());
  }

  @Test
  public void testBuildJoinOperationFullJoin() {
    Map<String, JoinKey> stringJoinKeyMap = new HashMap<>();
    configureJoinOperationTest(stringJoinKeyMap);

    JoinStage leftStage = JoinStage.builder("leftStage", null).setRequired(false).build();
    JoinStage rightStage = JoinStage.builder("rightStage", null).setRequired(false).build();

    helper.appendJoinOnKeyOperation(leftStage, rightStage, stringJoinKeyMap, false);
    Assert.assertEquals("FULL OUTER JOIN `some.bq.right` AS RIGHT ON some join condition",
                        builder.toString());
  }

  @Test
  public void testBuildJoinStatementSingleColumn() {
    JoinKey leftJoinKey = new JoinKey("anyTable", Collections.singletonList("left_a"));
    JoinKey rightJoinKey = new JoinKey("anyOtherTable", Collections.singletonList("right_x"));

    helper.appendJoinOnKeyStatement("leftTable", leftJoinKey, "rightTable", rightJoinKey, false);
    Assert.assertEquals("leftTable.left_a = rightTable.right_x",
                        builder.toString());
  }

  @Test
  public void testBuildJoinStatementSingleColumnNullSafe() {
    JoinKey leftJoinKey = new JoinKey("anyTable", Collections.singletonList("left_a"));
    JoinKey rightJoinKey = new JoinKey("anyOtherTable", Collections.singletonList("right_x"));

    helper.appendJoinOnKeyStatement("leftTable", leftJoinKey, "rightTable", rightJoinKey, true);
    Assert.assertEquals(
      "(leftTable.left_a = rightTable.right_x OR (leftTable.left_a IS NULL AND rightTable.right_x IS NULL))",
      builder.toString());
  }

  @Test
  public void testBuildJoinStatementMultipleColumns() {
    JoinKey leftJoinKey = new JoinKey("anyTable", Arrays.asList("left_a", "left_b", "left_c"));
    JoinKey rightJoinKey = new JoinKey("anyOtherTable", Arrays.asList("right_x", "right_y", "right_z"));

    helper.appendJoinOnKeyStatement("leftTable", leftJoinKey, "rightTable", rightJoinKey, true);
    Assert.assertEquals(
      "(leftTable.left_a = rightTable.right_x OR (leftTable.left_a IS NULL AND rightTable.right_x IS NULL)) AND "
        + "(leftTable.left_b = rightTable.right_y OR (leftTable.left_b IS NULL AND rightTable.right_y IS NULL)) AND "
        + "(leftTable.left_c = rightTable.right_z OR (leftTable.left_c IS NULL AND rightTable.right_z IS NULL))",
      builder.toString());
  }

  @Test
  public void testBuildEquals() {
    helper.appendEquals("leftTable", "leftField", "rightTable", "rightField", false);
    Assert.assertEquals("leftTable.leftField = rightTable.rightField",
                        builder.toString());
  }

  @Test
  public void testBuildEqualsNullSafe() {
    helper.appendEquals("leftTable", "leftField", "rightTable", "rightField", true);
    Assert.assertEquals("(leftTable.leftField = rightTable.rightField OR " +
                          "(leftTable.leftField IS NULL AND rightTable.rightField IS NULL))",
                        builder.toString());
  }

  @Test
  public void testGetFullTableName() {
    helper.addFullTableName("leftStage");
    Assert.assertEquals("`project.dataset.bqLeft`", helper.getFullTableName("leftStage"));
    helper.addFullTableName("rightStage");
    Assert.assertEquals("`project.dataset.bqRight`", helper.getFullTableName("rightStage"));
  }

  @Test
  public void testGetBQTableName() {
    Assert.assertEquals("bqLeft",
                        helper.getBQTableName("leftStage"));
    Assert.assertEquals("bqRight",
                        helper.getBQTableName("rightStage"));
  }

  @Test
  public void testBuildTableAlias() {
    Assert.assertEquals("A", helper.buildTableAlias(0));
    Assert.assertEquals("Z", helper.buildTableAlias(25));
    Assert.assertEquals("AA", helper.buildTableAlias(26));
    Assert.assertEquals("AB", helper.buildTableAlias(27));
    Assert.assertEquals("AY", helper.buildTableAlias(50));
    Assert.assertEquals("AZ", helper.buildTableAlias(51));
    Assert.assertEquals("BA", helper.buildTableAlias(52));
    Assert.assertEquals("BB", helper.buildTableAlias(53));
    Assert.assertEquals("BB", helper.buildTableAlias(53));
    Assert.assertEquals("ZY", helper.buildTableAlias(700));
    Assert.assertEquals("ZZ", helper.buildTableAlias(701));
    Assert.assertEquals("AAA", helper.buildTableAlias(702));
    Assert.assertEquals("AAB", helper.buildTableAlias(703));

    //Validate no collisions occur when generating aliases.
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < 2000; i++) {
      String alias = helper.buildTableAlias(i);
      Assert.assertFalse("Collision detected when generating aliases.", aliases.contains(alias));
      aliases.add(alias);
    }
  }

  @Test(expected = SQLEngineException.class)
  public void testGetBQTableNameThrowsException() {
    Assert.assertEquals("invalid",
                        helper.getBQTableName("undefinedStage"));
  }

  public void configureJoinOperationTest(Map<String, JoinKey> stringJoinKeyMap) {
    doAnswer(invocationOnMock -> {
      builder.append("some join condition");
      return null;
    })
      .when(helper)
      .appendJoinOnKeyStatement(anyString(),
                                any(JoinKey.class),
                                anyString(),
                                any(JoinKey.class),
                                anyBoolean());

    JoinKey leftJoinKey = new JoinKey("leftTable", Collections.singletonList("left_a"));
    JoinKey rightJoinKey = new JoinKey("rightTable", Collections.singletonList("right_x"));

    stringJoinKeyMap.put("leftStage", leftJoinKey);
    stringJoinKeyMap.put("rightStage", rightJoinKey);
  }

}
