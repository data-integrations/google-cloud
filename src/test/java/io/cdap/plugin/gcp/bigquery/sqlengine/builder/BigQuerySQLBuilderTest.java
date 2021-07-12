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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
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
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BigQuerySQLBuilderTest {
  private JoinDefinition joinDefinition;
  private StringBuilder builder;
  private Map<String, String> stageToBQTableNameMap;
  private Map<String, String> stageToFullTableNameMap;
  private Map<String, String> stageToTableAliasMap;
  private String project;
  private String dataset;
  private BigQuerySQLBuilder helper;

  @Before
  public void setUp() {
    joinDefinition = mock(JoinDefinition.class);
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

    helper = spy(new BigQuerySQLBuilder(joinDefinition,
                                        project,
                                        dataset,
                                        stageToBQTableNameMap,
                                        stageToFullTableNameMap,
                                        stageToTableAliasMap,
                                        builder));
  }

  @Test
  public void testFieldEqualityQuery() {
    // Inner join as both sides are required
    JoinStage users = JoinStage.builder("Users", null).setRequired(true).build();
    JoinStage purchases = JoinStage.builder("Purchases", null).setRequired(true).build();

    // Non null safe
    JoinCondition condition = JoinCondition.onKeys()
      .addKey(new JoinKey("Users", Arrays.asList("id")))
      .addKey(new JoinKey("Purchases", Arrays.asList("user_id")))
      .setNullSafe(false)
      .build();

    JoinDefinition joinDefinition = JoinDefinition.builder()
      .select(new JoinField("Users", "id", "user_id"),
              new JoinField("Purchases", "id", "purchase_id"))
      .from(users, purchases)
      .on(condition)
      .build();

    Map<String, String> stateToBqTableNames = new HashMap<>();
    stateToBqTableNames.put("Users", "u");
    stateToBqTableNames.put("Purchases", "p");

    BigQuerySQLBuilder helper =
      new BigQuerySQLBuilder(joinDefinition, "my-project", "MY_DS", stateToBqTableNames);

    Assert.assertEquals(
      "SELECT `Users`.id AS `user_id` , `Purchases`.id AS `purchase_id` "
        + "FROM `my-project.MY_DS.u` AS `Users` "
        + "INNER JOIN `my-project.MY_DS.p` AS `Purchases` ON `Users`.id = `Purchases`.user_id",
      helper.getQuery());
  }

  @Test
  public void testFieldEqualityQueryMultipleTables() {
    // First join is a right join, second join is a left join
    JoinStage shipments = JoinStage.builder("Shipments", null).setRequired(false).build();
    JoinStage fromAddresses = JoinStage.builder("FromAddresses", null).setRequired(true).build();
    JoinStage toAddresses = JoinStage.builder("ToAddresses", null).setRequired(false).build();

    // null safe
    JoinCondition condition = JoinCondition.onKeys()
      .addKey(new JoinKey("Shipments", Arrays.asList("id")))
      .addKey(new JoinKey("FromAddresses", Arrays.asList("shipment_id")))
      .addKey(new JoinKey("ToAddresses", Arrays.asList("shipment_id")))
      .setNullSafe(true)
      .build();

    JoinDefinition joinDefinition = JoinDefinition.builder()
      .select(new JoinField("Shipments", "id", "shipment_id"),
              new JoinField("FromAddresses", "zip", "from_zip"),
              new JoinField("ToAddresses", "zip", "to_zip"))
      .from(shipments, fromAddresses, toAddresses)
      .on(condition)
      .build();

    Map<String, String> stateToBqTableNames = new HashMap<>();
    stateToBqTableNames.put("Shipments", "shipments");
    stateToBqTableNames.put("FromAddresses", "from-addr");
    stateToBqTableNames.put("ToAddresses", "to-addr");

    BigQuerySQLBuilder helper =
      new BigQuerySQLBuilder(joinDefinition, "my-project", "MY_DS", stateToBqTableNames);

    Assert.assertEquals(
      "SELECT `Shipments`.id AS `shipment_id` , `FromAddresses`.zip AS `from_zip` , `ToAddresses`.zip AS `to_zip` "
        + "FROM `my-project.MY_DS.shipments` AS `Shipments` "
        + "RIGHT OUTER JOIN `my-project.MY_DS.from-addr` AS `FromAddresses` "
        // Null safe join keys
        + "ON `Shipments`.id IS NOT DISTINCT FROM `FromAddresses`.shipment_id "
        + "LEFT OUTER JOIN `my-project.MY_DS.to-addr` AS `ToAddresses` "
        // Null safe join keys
        + "ON `FromAddresses`.shipment_id IS NOT DISTINCT FROM `ToAddresses`.shipment_id",
      helper.getQuery());
  }

  @Test
  public void testOnExpressionQuery() {
    Schema usersSchema = Schema.recordOf("Users",
                                         Schema.Field.of("id", Schema.of(Schema.Type.INT)));
    Schema purchasesSchema = Schema.recordOf("Purchases",
                                             Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                             Schema.Field.of("user_id", Schema.of(Schema.Type.INT)),
                                             Schema.Field.of("price", Schema.of(Schema.Type.INT)));

    // Inner join as both sides are required
    JoinStage users = JoinStage.builder("Users", usersSchema).setRequired(true).build();
    JoinStage purchases = JoinStage.builder("Purchases", purchasesSchema).setRequired(true).build();

    // Non null safe
    JoinCondition condition = JoinCondition.onExpression()
      .setExpression("Users.id = purch.user_id and `purch`.price > 100")
      .addDatasetAlias("Purchases", "purch")
      .build();

    JoinDefinition joinDefinition = JoinDefinition.builder()
      .select(new JoinField("Users", "id", "user_id"),
              new JoinField("Purchases", "id", "purchase_id"),
              new JoinField("Purchases", "price"))
      .from(users, purchases)
      .on(condition)
      .build();

    Map<String, String> stateToBqTableNames = new HashMap<>();
    stateToBqTableNames.put("Users", "u");
    stateToBqTableNames.put("Purchases", "p");

    BigQuerySQLBuilder helper =
      new BigQuerySQLBuilder(joinDefinition, "my-project", "MY_DS", stateToBqTableNames);

    Assert.assertEquals(
      "SELECT `Users`.id AS `user_id` , `purch`.id AS `purchase_id` , `purch`.price "
        + "FROM `my-project.MY_DS.u` AS `Users` "
        + "INNER JOIN `my-project.MY_DS.p` AS `purch` "
        + "ON Users.id = purch.user_id and `purch`.price > 100",
      helper.getQuery());
  }

  @Test
  public void testBuildSelectedFields() {
    JoinField joinField1 = new JoinField("leftStage", "field1");
    JoinField joinField2 = new JoinField("rightStage", "field2", "alias2");
    JoinField joinField3 = new JoinField("rightStage", "field3");
    JoinField joinField4 = new JoinField("leftStage", "field4", "alias4");

    when(joinDefinition.getSelectedFields()).thenReturn(Arrays.asList(joinField1, joinField2, joinField3, joinField4));

    Assert.assertEquals("LEFT.field1 , RIGHT.field2 AS `alias2` , RIGHT.field3 , LEFT.field4 AS `alias4`",
                        helper.getSelectedFields());
  }

  @Test
  public void testBuildSelectedField() {
    JoinField joinField = new JoinField("leftStage", "field");
    Assert.assertEquals("LEFT.field",
                        helper.buildSelectedField(joinField));
  }

  @Test
  public void testBuildSelectedFieldWithAlias() {
    JoinField joinField = new JoinField("rightStage", "field", "alias");
    Assert.assertEquals("RIGHT.field AS `alias`",
                        helper.buildSelectedField(joinField));
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
                          + " LEFT OUTER JOIN `bq.stage.2` AS T2 ON T1.a IS NOT DISTINCT FROM T2.b"
                          + " FULL OUTER JOIN `bq.stage.3` AS T3 ON T2.b IS NOT DISTINCT FROM T3.c"
                          + " RIGHT OUTER JOIN `bq.stage.4` AS T4 ON T3.c IS NOT DISTINCT FROM T4.d"
                          + " INNER JOIN `bq.stage.5` AS T5 ON T4.d IS NOT DISTINCT FROM T5.e",
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
  public void testBuildJoinOnKeyStatementSingleColumn() {
    JoinKey leftJoinKey = new JoinKey("anyTable", Collections.singletonList("left_a"));
    JoinKey rightJoinKey = new JoinKey("anyOtherTable", Collections.singletonList("right_x"));

    helper.appendJoinOnKeyClause("leftTable", leftJoinKey, "rightTable", rightJoinKey, false);
    Assert.assertEquals("leftTable.left_a = rightTable.right_x",
                        builder.toString());
  }

  @Test
  public void testBuildJoinOnKeyStatementSingleColumnNullSafe() {
    JoinKey leftJoinKey = new JoinKey("anyTable", Collections.singletonList("left_a"));
    JoinKey rightJoinKey = new JoinKey("anyOtherTable", Collections.singletonList("right_x"));

    helper.appendJoinOnKeyClause("leftTable", leftJoinKey, "rightTable", rightJoinKey, true);
    Assert.assertEquals(
      "leftTable.left_a IS NOT DISTINCT FROM rightTable.right_x",
      builder.toString());
  }

  @Test
  public void testBuildJoinOnKeyStatementMultipleColumns() {
    JoinKey leftJoinKey = new JoinKey("anyTable", Arrays.asList("left_a", "left_b", "left_c"));
    JoinKey rightJoinKey = new JoinKey("anyOtherTable", Arrays.asList("right_x", "right_y", "right_z"));

    helper.appendJoinOnKeyClause("leftTable", leftJoinKey, "rightTable", rightJoinKey, true);
    Assert.assertEquals(
      "leftTable.left_a IS NOT DISTINCT FROM rightTable.right_x AND "
        + "leftTable.left_b IS NOT DISTINCT FROM rightTable.right_y AND "
        + "leftTable.left_c IS NOT DISTINCT FROM rightTable.right_z",
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
    Assert.assertEquals("leftTable.leftField IS NOT DISTINCT FROM rightTable.rightField",
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
  public void testQuoteAlias() {
    Assert.assertEquals("`stage`", helper.quoteAlias("stage"));
    Assert.assertEquals("`Some_stage`", helper.quoteAlias("Some_stage"));
    Assert.assertEquals("`Some-other.Stage`", helper.quoteAlias("Some-other.Stage"));
  }

  @Test
  public void testGetBQTableName() {
    Assert.assertEquals("bqLeft",
                        helper.getBQTableName("leftStage"));
    Assert.assertEquals("bqRight",
                        helper.getBQTableName("rightStage"));
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
      .appendJoinOnKeyClause(anyString(),
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
