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

package io.cdap.plugin.gcp.bigquery.sqlengine;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinDefinition;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BigQuerySQLEngine.class, LoggerFactory.class})
public class BigQuerySQLEngineTest {

  static Logger logger;

  @BeforeClass
  public static void beforeClass() {
    mockStatic(LoggerFactory.class);
    logger = mock(Logger.class);
    when(LoggerFactory.getLogger(ArgumentMatchers.any(Class.class))).thenReturn(logger);
  }

  @Before
  public void setup() {
    reset(logger);
  }

  @Test
  public void testIsValidJoinDefinitionOnKey() {
    Schema shipmentSchema =
      Schema.recordOf("Shipments",
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)));

    Schema fromAddressSchema =
      Schema.recordOf("FromAddress",
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("shipment_id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("zip", Schema.nullableOf(Schema.of(Schema.Type.INT))));

    Schema toAddressSchema =
      Schema.recordOf("ToAddress",
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("shipment_id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("zip", Schema.nullableOf(Schema.of(Schema.Type.INT))));

    Schema outputSchema =
      Schema.recordOf("Join",
                      Schema.Field.of("shipment_id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("from_zip", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                      Schema.Field.of("to_zip", Schema.nullableOf(Schema.of(Schema.Type.INT))));

    // First join is a right join, second join is a left join
    JoinStage shipments = JoinStage.builder("Shipments", shipmentSchema).setRequired(true).build();
    JoinStage fromAddresses = JoinStage.builder("FromAddress", fromAddressSchema).setRequired(true).build();
    JoinStage toAddresses = JoinStage.builder("ToAddress", toAddressSchema).setRequired(true).build();

    // null safe
    JoinCondition condition = JoinCondition.onKeys()
      .addKey(new JoinKey("Shipments", Arrays.asList("id")))
      .addKey(new JoinKey("FromAddress", Arrays.asList("shipment_id")))
      .addKey(new JoinKey("ToAddress", Arrays.asList("shipment_id")))
      .setNullSafe(false)
      .build();

    JoinDefinition joinDefinition = JoinDefinition.builder()
      .select(new JoinField("Shipments", "id", "shipment_id"),
              new JoinField("FromAddress", "zip", "from_zip"),
              new JoinField("ToAddress", "zip", "to_zip"))
      .from(shipments, fromAddresses, toAddresses)
      .on(condition)
      .setOutputSchemaName("Join")
      .setOutputSchema(outputSchema)
      .build();

    SQLJoinDefinition sqlJoinDefinition = new SQLJoinDefinition("Join", joinDefinition);

    Assert.assertTrue(BigQuerySQLEngine.isValidJoinDefinition(sqlJoinDefinition));
    verify(logger, times(0)).warn(anyString(), anyString(), anyString());
  }

  @Test
  public void testIsValidJoinDefinitionOnKeyWithErrors() {
    ArgumentCaptor<String> messageTemplateCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> stageNameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> issuesCaptor = ArgumentCaptor.forClass(String.class);

    Schema shipmentSchema =
      Schema.recordOf("Shipments",
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("enum_shipment", Schema.enumWith("a", "b")));

    Schema fromAddressSchema =
      Schema.recordOf("FromAddress",
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("shipment_id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("zip", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("enum_from", Schema.enumWith("a", "b")));

    Schema toAddressSchema =
      Schema.recordOf("ToAddress",
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("shipment_id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("zip", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("enum_to", Schema.enumWith("a", "b")));

    Schema outputSchema =
      Schema.recordOf("Join",
                      Schema.Field.of("shipment_id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("from_zip", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("to_zip", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("enum_out", Schema.enumWith("a", "b")));

    // First join is a right join, second join is a left join
    JoinStage shipments = JoinStage.builder("Shipments", shipmentSchema).setRequired(true).build();
    JoinStage fromAddresses = JoinStage.builder("FromAddress", fromAddressSchema).setRequired(true).build();
    JoinStage toAddresses = JoinStage.builder("ToAddress", toAddressSchema).setRequired(true).build();

    // null safe
    JoinCondition condition = JoinCondition.onKeys()
      .addKey(new JoinKey("Shipments", Arrays.asList("id")))
      .addKey(new JoinKey("FromAddress", Arrays.asList("shipment_id")))
      .addKey(new JoinKey("ToAddress", Arrays.asList("shipment_id")))
      .setNullSafe(false)
      .build();

    JoinDefinition joinDefinition = JoinDefinition.builder()
      .select(new JoinField("Shipments", "id", "shipment_id"),
              new JoinField("FromAddress", "zip", "from_zip"),
              new JoinField("ToAddress", "zip", "to_zip"),
              new JoinField("Shipments", "enum_shipment", "enum_out"))
      .from(shipments, fromAddresses, toAddresses)
      .on(condition)
      .setOutputSchemaName("Join")
      .setOutputSchema(outputSchema)
      .build();

    SQLJoinDefinition sqlJoinDefinition = new SQLJoinDefinition("Join", joinDefinition);

    Assert.assertFalse(BigQuerySQLEngine.isValidJoinDefinition(sqlJoinDefinition));
    verify(logger).warn(messageTemplateCaptor.capture(), stageNameCaptor.capture(), issuesCaptor.capture());

    String messageTemplate = messageTemplateCaptor.getValue();
    Assert.assertTrue(messageTemplate.contains(
      "Join operation for stage '{}' could not be executed in BigQuery. Issues found:"));

    String stageName = stageNameCaptor.getValue();
    Assert.assertEquals("Join", stageName);

    String issues = issuesCaptor.getValue();
    Assert.assertTrue(issues.contains("Input schema from stage 'Shipments' contains unsupported field types "
                                        + "for the following fields: enum_shipment"));
    Assert.assertTrue(issues.contains("Input schema from stage 'FromAddress' contains unsupported field types "
                                        + "for the following fields: enum_from"));
    Assert.assertTrue(issues.contains("Input schema from stage 'ToAddress' contains unsupported field types "
                                        + "for the following fields: enum_to"));
    Assert.assertTrue(issues.contains("Output schema contains unsupported field types "
                                        + "for the following fields: enum_out"));
  }

  @Test
  public void testIsValidJoinDefinitionOnExpression() {
    Schema shipmentSchema =
      Schema.recordOf("Shipments",
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)));

    Schema fromAddressSchema =
      Schema.recordOf("FromAddress",
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("shipment_id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("zip", Schema.of(Schema.Type.INT)));

    Schema outputSchema =
      Schema.recordOf("Join",
                      Schema.Field.of("shipment_id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("from_zip", Schema.of(Schema.Type.INT)));

    // First join is a right join, second join is a left join
    JoinStage shipments = JoinStage.builder("Shipments", shipmentSchema).setRequired(true).build();
    JoinStage fromAddresses = JoinStage.builder("FromAddress", fromAddressSchema).setRequired(true).build();

    JoinCondition condition = JoinCondition.onExpression()
      .setExpression("`shipments`.id = `from``addresses`.shipment_id")
      .addDatasetAlias("Shipments", "shp")
      .build();

    JoinDefinition joinDefinition = JoinDefinition.builder()
      .select(new JoinField("Shipments", "id", "shipment_id"),
              new JoinField("FromAddress", "zip", "from_zip"))
      .from(shipments, fromAddresses)
      .on(condition)
      .setOutputSchemaName("Join")
      .setOutputSchema(outputSchema)
      .build();

    SQLJoinDefinition sqlJoinDefinition = new SQLJoinDefinition("Join", joinDefinition);

    Assert.assertTrue(BigQuerySQLEngine.isValidJoinDefinition(sqlJoinDefinition));
    verify(logger, times(0)).warn(anyString(), anyString(), anyString());
  }

  @Test
  public void testIsValidJoinDefinitionOnExpressionWithErrors() {
    ArgumentCaptor<String> messageTemplateCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> stageNameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> issuesCaptor = ArgumentCaptor.forClass(String.class);

    Schema shipmentSchema =
      Schema.recordOf("Ship\\ments",
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)));

    Schema fromAddressSchema =
      Schema.recordOf("From`Address",
                      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("shipment_id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("zip", Schema.of(Schema.Type.INT)));

    Schema outputSchema =
      Schema.recordOf("Join",
                      Schema.Field.of("shipment_id", Schema.of(Schema.Type.INT)),
                      Schema.Field.of("from_zip", Schema.of(Schema.Type.INT)));

    // First join is a right join, second join is a left join
    JoinStage shipments = JoinStage.builder("Ship\\ments", shipmentSchema).setRequired(true).build();
    JoinStage fromAddresses = JoinStage.builder("From`Address", fromAddressSchema).setRequired(true).build();

    JoinCondition condition = JoinCondition.onExpression()
      .setExpression("`shi\\pments`.id = `from``addresses`.shipment_id")
      .addDatasetAlias("Ship\\ments", "Shi\\\\pments")
      .build();

    JoinDefinition joinDefinition = JoinDefinition.builder()
      .select(new JoinField("Ship\\ments", "id", "shipment_id"),
              new JoinField("From`Address", "zip", "from_zip"))
      .from(shipments, fromAddresses)
      .on(condition)
      .setOutputSchemaName("Join")
      .setOutputSchema(outputSchema)
      .build();

    SQLJoinDefinition sqlJoinDefinition = new SQLJoinDefinition("Join", joinDefinition);

    Assert.assertFalse(BigQuerySQLEngine.isValidJoinDefinition(sqlJoinDefinition));
    verify(logger).warn(messageTemplateCaptor.capture(), stageNameCaptor.capture(), issuesCaptor.capture());

    String messageTemplate = messageTemplateCaptor.getValue();
    Assert.assertTrue(messageTemplate.contains(
      "Join operation for stage '{}' could not be executed in BigQuery. Issues found:"));

    String stageName = stageNameCaptor.getValue();
    Assert.assertEquals("Join", stageName);

    String issues = issuesCaptor.getValue();
    Assert.assertTrue(issues.contains(
      "Unsupported stage name 'Ship\\ments'. Stage names cannot contain backtick ` or backslash \\ "));
    Assert.assertTrue(issues.contains(
      "Unsupported stage name 'From`Address'. Stage names cannot contain backtick ` or backslash \\ "));
    Assert.assertTrue(issues.contains("Unsupported alias 'Shi\\\\pments' for stage 'Ship\\ments'"));
  }

  @Test
  public void testSupportsSchema() {
    Schema schemaWithEnum = Schema.recordOf("Users",
                                            Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("enum", Schema.enumWith("a", "b")));
    BigQuerySQLEngine.SchemaValidation schemaWithEnumValidation = BigQuerySQLEngine.validateSchema(schemaWithEnum);
    Assert.assertFalse(schemaWithEnumValidation.isSupported());
    Assert.assertEquals(1, schemaWithEnumValidation.getInvalidFields().size());
    Assert.assertEquals("enum", schemaWithEnumValidation.getInvalidFields().get(0));

    Schema schemaWithMap = Schema.recordOf("Users",
                                           Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                           Schema.Field.of("map",
                                                           Schema.mapOf(Schema.of(Schema.Type.INT),
                                                                        Schema.of(Schema.Type.INT))));
    BigQuerySQLEngine.SchemaValidation schemaWithMapValidation = BigQuerySQLEngine.validateSchema(schemaWithMap);
    Assert.assertFalse(schemaWithMapValidation.isSupported());
    Assert.assertEquals(1, schemaWithMapValidation.getInvalidFields().size());
    Assert.assertEquals("map", schemaWithMapValidation.getInvalidFields().get(0));

    Schema schemaWithUnion = Schema.recordOf("Users",
                                             Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                             Schema.Field.of("union",
                                                             Schema.unionOf(Schema.of(Schema.Type.INT),
                                                                            Schema.of(Schema.Type.LONG))));
    BigQuerySQLEngine.SchemaValidation schemaWithUnionValidation = BigQuerySQLEngine.validateSchema(schemaWithUnion);
    Assert.assertFalse(schemaWithUnionValidation.isSupported());
    Assert.assertEquals(1, schemaWithUnionValidation.getInvalidFields().size());
    Assert.assertEquals("union", schemaWithUnionValidation.getInvalidFields().get(0));

    Schema schemaWithValidFields = Schema.recordOf("Users",
                                                   Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                                   Schema.Field.of("null", Schema.of(Schema.Type.NULL)),
                                                   Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
                                                   Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
                                                   Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
                                                   Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
                                                   Schema.Field.of("bytes", Schema.of(Schema.Type.BYTES)),
                                                   Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
                                                   Schema.Field.of("array",
                                                                   Schema.arrayOf(Schema.of(Schema.Type.STRING))),
                                                   Schema.Field.of("record",
                                                                   Schema.recordOf("some_record")));
    BigQuerySQLEngine.SchemaValidation schemaWithValidFieldsValidation =
      BigQuerySQLEngine.validateSchema(schemaWithValidFields);
    Assert.assertTrue(schemaWithValidFieldsValidation.isSupported());
    Assert.assertEquals(0, schemaWithValidFieldsValidation.getInvalidFields().size());
  }

  @Test
  public void testIsValidIdentifier() {
    Assert.assertFalse(BigQuerySQLEngine.isValidIdentifier(null));
    Assert.assertFalse(BigQuerySQLEngine.isValidIdentifier("ab`c"));
    Assert.assertFalse(BigQuerySQLEngine.isValidIdentifier("ab\\c"));
    Assert.assertTrue(BigQuerySQLEngine.isValidIdentifier("abc"));
    Assert.assertTrue(BigQuerySQLEngine.isValidIdentifier("áüé"));
    Assert.assertTrue(BigQuerySQLEngine.isValidIdentifier("コンピューター"));
    Assert.assertTrue(BigQuerySQLEngine.isValidIdentifier("电脑"));
  }
}
