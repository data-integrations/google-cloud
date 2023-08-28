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
package io.cdap.plugin.bigquery.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.utils.E2EHelper;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Then;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

/**
 * BigQuery related common stepDesigns.
 */
public class BigQueryBase implements E2EHelper {

  static {
    SeleniumHelper.getPropertiesLocators(CdfStudioLocators.class);
  }

  @Then("Enter BigQuery property reference name")
  public void enterBigQueryPropertyReferenceName() {
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_Ref_" + UUID.randomUUID());
  }

  @Then("Enter BigQuery property projectId {string}")
  public void enterBigQueryPropertyProjectId(String projectId) throws IOException {
    CdfBigQueryPropertiesActions.enterProjectId(PluginPropertyUtils.pluginProp(projectId));
  }

  @Then("Enter BigQuery property datasetProjectId {string}")
  public void enterBigQueryPropertyDatasetProjectId(String datasetProjectId) throws IOException {
    CdfBigQueryPropertiesActions.enterDatasetProjectId(PluginPropertyUtils.pluginProp(datasetProjectId));
  }

  @Then("Enter BigQuery property dataset {string}")
  public void enterBigQueryPropertyDataset(String dataset) {
    CdfBigQueryPropertiesActions.enterBigQueryDataset(PluginPropertyUtils.pluginProp(dataset));
  }

  @Then("Enter BigQuery property table {string}")
  public void enterBigQueryPropertyTable(String table) {
    CdfBigQueryPropertiesActions.enterBigQueryTable(PluginPropertyUtils.pluginProp(table));
  }

  @Then("Enter BiqQuery property encryption key name {string} if cmek is enabled")
  public void enterBiqQueryPropertyEncryptionKeyNameIfCmekIsEnabled(String cmek) throws IOException {
    String cmekBQ = PluginPropertyUtils.pluginProp(cmek);
    if (cmekBQ != null) {
      CdfBigQueryPropertiesActions.enterCmekProperty(cmekBQ);
      BeforeActions.scenario.write("Entered encryption key name - " + cmekBQ);
    }
  }

  @Then("Close the BigQuery properties")
  public void closeTheBigQueryProperties() {
    CdfStudioActions.clickCloseButton();
  }

  @Then("Get count of no of records transferred to target BigQuery Table")
  public void getCountOfNoOfRecordsTransferredToTargetBigQueryTable() throws IOException, InterruptedException {
    int countRecords = BigQueryClient.countBqQuery(TestSetupHooks.bqTargetTable);
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    Assert.assertEquals("Number of records transferred should be equal to records out ",
                        countRecords, recordOut());
  }

  @Then("Validate records transferred to target table is equal to number of records from source table " +
    "with filter {string}")
  public void validateRecordsTransferredToTargetTableIsEqualToNumberOfRecordsFromSourceTableWithFilter(String filter)
    throws IOException, InterruptedException {
    String projectId = (PluginPropertyUtils.pluginProp("projectId"));
    String datasetName = (PluginPropertyUtils.pluginProp("dataset"));
    int countRecordsTarget = BigQueryClient.countBqQuery(TestSetupHooks.bqTargetTable);
    String selectQuery = "SELECT count(*)  FROM `" + projectId + "." + datasetName + "." +
      TestSetupHooks.bqTargetTable + "` WHERE " + PluginPropertyUtils.pluginProp(filter);
    Optional<String> result = BigQueryClient.getSoleQueryResult(selectQuery);
    int count = result.map(Integer::parseInt).orElse(0);
    BeforeActions.scenario.write("Number of records transferred with respect to filter:" + count);
    Assert.assertEquals(count, countRecordsTarget);
  }

  @Then("Validate partition date in output partitioned table")
  public void validatePartitionDateInOutputPartitionedTable()
    throws IOException, InterruptedException {
    Optional<String> result = BigQueryClient
      .getSoleQueryResult("SELECT distinct  _PARTITIONDATE as pt FROM `" +
                            (PluginPropertyUtils.pluginProp("projectId")) + "." +
                            (PluginPropertyUtils.pluginProp("dataset")) + "." +
                            TestSetupHooks.bqTargetTable +
                            "` WHERE _PARTITION_LOAD_TIME IS Not NULL ORDER BY _PARTITIONDATE DESC ");
    String outputDate = StringUtils.EMPTY;
    if (result.isPresent()) {
      outputDate = result.get();
    }
    BeforeActions.scenario.write("Partitioned date in output record:" + outputDate);
    Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd").format(new Date()), outputDate);
  }

  @Then("Validate the records are not created in output table")
  public void validateTheRecordsAreNotCreatedInOutputTable() throws IOException, InterruptedException {
    int countRecords = BigQueryClient.countBqQuery(TestSetupHooks.bqTargetTable);
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    Assert.assertEquals(0, countRecords);
  }

  @Then("Validate partitioning is not done on the output table")
  public void validatePartitioningIsNotDoneOnTheOutputTable() {
    try {
      BigQueryClient.getSoleQueryResult("SELECT distinct  _PARTITIONDATE as pt FROM `" +
                                          (PluginPropertyUtils.pluginProp("projectId"))
                                          + "." + (PluginPropertyUtils.pluginProp("dataset")) + "." +
                                          TestSetupHooks.bqTargetTable
                                          + "` WHERE _PARTITION_LOAD_TIME IS Not NULL ");
    } catch (Exception e) {
      String partitionException = e.toString();
      Assert.assertTrue(partitionException.contains("Unrecognized name: _PARTITION_LOAD_TIME"));
      BeforeActions.scenario.write("Partition Not Created" + partitionException);
    }
  }

  @Then("Enter the BigQuery properties with blank property {string}")
  public void enterTheBigQueryPropertiesWithBlankProperty(String property) {
    if (!property.equalsIgnoreCase("referenceName")) {
      CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_" + UUID.randomUUID());
    }
    if (!property.equalsIgnoreCase("dataset")) {
      CdfBigQueryPropertiesActions.enterBigQueryDataset(PluginPropertyUtils.pluginProp("dataset"));
    }
    if (!property.equalsIgnoreCase("table")) {
      CdfBigQueryPropertiesActions.enterBigQueryTable("dummyTable");
    }
    if (!PluginPropertyUtils.pluginProp("bqMandatoryProperties").contains(property)) {
      Assert.fail("Invalid BigQuery mandatory property " + property);
    }
  }

  @Then("Validate the cmek key {string} of target BigQuery table if cmek is enabled")
  public void validateTheCmekKeyOfTargetBigQueryTableIfCmekIsEnabled(String cmek) throws IOException {
    String cmekBQ = PluginPropertyUtils.pluginProp(cmek);
    if (cmekBQ != null) {
      Assert.assertTrue("Cmek key of target BigQuery table should be equal to " +
                          "cmek key provided in config file",
                        BigQueryClient.verifyCmekKey(TestSetupHooks.bqTargetTable, cmekBQ));
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Enter BigQuery property {string} as macro argument {string}")
  public void enterBigQueryPropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    enterPropertyAsMacroArgument(pluginProperty, macroArgument);
  }

  @Then("Enter BigQuery cmek property {string} as macro argument {string} if cmek is enabled")
  public void enterBigQueryCmekPropertyAsMacroArgumentIfCmekIsEnabled(String pluginProperty, String macroArgument) {
    String cmekBQ = PluginPropertyUtils.pluginProp("cmekBQ");
    if (cmekBQ != null) {
      enterPropertyAsMacroArgument(pluginProperty, macroArgument);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Enter runtime argument value {string} for BigQuery cmek property key {string} if BQ cmek is enabled")
  public void enterRuntimeArgumentValueForBigQueryCmekPropertyKeyIfBQCmekIsEnabled(String value,
                                                                                   String runtimeArgumentKey) {
    String cmekBQ = PluginPropertyUtils.pluginProp(value);
    if (cmekBQ != null) {
      ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey), cmekBQ);
      BeforeActions.scenario.write("BigQuery encryption key name - " + cmekBQ);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Verify the partition table is created with partitioned on field {string}")
  public void verifyThePartitionTableIsCreatedWithPartitionedOnField(String partitioningField) throws IOException,
    InterruptedException {
    Optional<String> result = BigQueryClient
      .getSoleQueryResult("SELECT IS_PARTITIONING_COLUMN FROM `" +
                            (PluginPropertyUtils.pluginProp("projectId")) + "."
                            + (PluginPropertyUtils.pluginProp("dataset")) + ".INFORMATION_SCHEMA.COLUMNS` " +
                            "WHERE table_name = '" + TestSetupHooks.bqTargetTable
                            + "' and column_name = '" + PluginPropertyUtils.pluginProp(partitioningField) + "' ");
    String isPartitioningDoneOnField = StringUtils.EMPTY;
    if (result.isPresent()) {
      isPartitioningDoneOnField = result.get();
    }
    BeforeActions.scenario.write("Is Partitioning done on column :" + isPartitioningDoneOnField);
    Assert.assertEquals("yes", isPartitioningDoneOnField.toLowerCase());
  }

  @Then("Enter BigQuery property temporary bucket name {string}")
  public void enterBigQuerySinkPropertyTemporaryBucketName(String temporaryBucket) throws IOException {
    CdfBigQueryPropertiesActions.enterTemporaryBucketName(PluginPropertyUtils.pluginProp(temporaryBucket));
  }

  @Then("Verify the BigQuery validation error message for invalid property {string}")
  public void verifyTheBigQueryValidationErrorMessageForInvalidProperty(String property) {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage;
    if (property.equalsIgnoreCase("gcsChunkSize")) {
      expectedErrorMessage = PluginPropertyUtils
        .errorProp(E2ETestConstants.ERROR_MSG_BQ_INCORRECT_CHUNKSIZE);
    } else if (property.equalsIgnoreCase("bucket")) {
      expectedErrorMessage = PluginPropertyUtils
        .errorProp(E2ETestConstants.ERROR_MSG_BQ_INCORRECT_TEMPORARY_BUCKET);
    } else if (property.equalsIgnoreCase("table")) {
      expectedErrorMessage = PluginPropertyUtils
        .errorProp(E2ETestConstants.ERROR_MSG_INCORRECT_TABLE_NAME);
    } else {
      expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_BQ_INCORRECT_PROPERTY).
        replaceAll("PROPERTY", property.substring(0, 1).toUpperCase() + property.substring(1));
    }
    String actualErrorMessage = PluginPropertyUtils.findPropertyErrorElement(property).getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = PluginPropertyUtils.getErrorColor(PluginPropertyUtils.findPropertyErrorElement(property));
    String expectedColor = ConstantsUtil.ERROR_MSG_COLOR;
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Validate records transferred to target table is equal to number of records from source table")
  public void validateRecordsTransferredToTargetTableIsEqualToNumberOfRecordsFromSourceTable()
    throws IOException, InterruptedException {
    int countRecordsTarget = BigQueryClient.countBqQuery(TestSetupHooks.bqTargetTable);
    Optional<String> result = BigQueryClient.getSoleQueryResult("SELECT count(*)  FROM `" +
                                                                  (PluginPropertyUtils.pluginProp("projectId"))
                                                                  + "." + (PluginPropertyUtils.pluginProp
      ("dataset")) + "." + TestSetupHooks.bqTargetTable + "` ");
    int count = result.map(Integer::parseInt).orElse(0);
    BeforeActions.scenario.write("Number of records transferred from source table to target table:" + count);
    Assert.assertEquals(count, countRecordsTarget);
  }
}
