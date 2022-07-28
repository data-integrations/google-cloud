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

package io.cdap.plugin.bigquerymultitable.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPluginPropertiesActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.bigquerymultitable.actions.BQMTActions;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.utils.E2EHelper;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;

/**
 * BQMT Sink Plugin related step design.
 */
public class BQMTSink implements E2EHelper {

  @When("Sink is BiqQueryMultiTable")
  public void sinkIsBigQueryMultiTable() {
    CdfStudioActions.clickSink();
    selectSinkPlugin("BigQueryMultiTable");
  }

  @Then("Open the BiqQueryMultiTable sink properties")
  public void openTheBigQueryMultiTableSinkProperties() {
    openSinkPluginProperties("BigQueryMultiTable");
  }

  @Then("Enter the BiqQueryMultiTable sink properties with blank property {string}")
  public void enterTheBigQueryMultiTableSinkPropertiesWithBlankProperty(String property) {
    if (property.equalsIgnoreCase("referenceName")) {
      BQMTActions.enterBQMTDataset("dummyDataset");
    } else if (property.equalsIgnoreCase("dataset")) {
      BQMTActions.enterBQMTReferenceName();
    } else {
      Assert.fail("Invalid BQMT Mandatory Field : " + property);
    }
  }

  @Then("Enter BiqQueryMultiTable sink property projectId {string}")
  public void enterBigQueryMultiTableSinkPropertyProjectId(String projectId) {
    BQMTActions.enterProjectID(PluginPropertyUtils.pluginProp(projectId));
  }

  @Then("Enter BiqQueryMultiTable sink property reference name")
  public void enterBigQueryMultiTableSinkPropertyReferenceName() {
    BQMTActions.enterBQMTReferenceName();
  }

  @Then("Enter BiqQueryMultiTable sink property dataset {string}")
  public void enterBigQueryMultiTableSinkPropertyDataset(String dataset) {
    BQMTActions.enterBQMTDataset(PluginPropertyUtils.pluginProp(dataset));
  }

  @Then("Close the BiqQueryMultiTable properties")
  public void closeTheBigQueryMultiTableProperties() {
    BQMTActions.close();
  }

  @Then("Toggle BiqQueryMultiTable sink property truncateTable to {string}")
  public void toggleBigQueryMultiTableSinkPropertyTruncateTableTo(String toggle) {
    if(toggle.equalsIgnoreCase("True")){
      BQMTActions.clickTruncateTableSwitch();
    }
  }

  @Then("Toggle BiqQueryMultiTable sink property allow flexible schema to {string}")
  public void toggleBigQueryMultiTableSinkPropertyAllowFlexibleSchemaTo(String toggle) {
    if(toggle.equalsIgnoreCase("True")){
      BQMTActions.clickAllowFlexibleSchemaSwitch();
    }
  }

  @Then("Enter BiqQueryMultiTable sink property GCS upload request chunk size {string}")
  public void enterBigQueryMultiTableSinkPropertyGCSUploadRequestChunkSize(String chunkSize) {
    BQMTActions.enterChunkSize(PluginPropertyUtils.pluginProp(chunkSize));
  }

  @Then("Verify the BiqQueryMultiTable sink validation error message for invalid property {string}")
  public void verifyTheBigQueryMultiTableSinkValidationErrorMessageForInvalidProperty(String property) {
    CdfPluginPropertiesActions.clickValidateButton();
    String expectedErrorMessage;
    if (property.equalsIgnoreCase("gcsChunkSize")) {
      expectedErrorMessage = PluginPropertyUtils
        .errorProp(E2ETestConstants.ERROR_MSG_BQMT_INCORRECT_CHUNKSIZE);
    } else if (property.equalsIgnoreCase("bucket")) {
      expectedErrorMessage = PluginPropertyUtils
        .errorProp(E2ETestConstants.ERROR_MSG_BQMT_INCORRECT_TEMPORARY_BUCKET);
    } else if (property.equalsIgnoreCase("dataset")) {
      expectedErrorMessage = PluginPropertyUtils
        .errorProp(E2ETestConstants.ERROR_MSG_BQMT_INCORRECT_DATASET);
    } else {
      expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_BQMT_INCORRECT_REFERENCENAME).
        replace("REFERENCE_NAME", PluginPropertyUtils.pluginProp("bqmtInvalidSinkReferenceName"));
    }
    CdfPluginPropertiesActions.verifyPluginPropertyInlineErrorMessage(property, expectedErrorMessage);
    CdfPluginPropertiesActions.verifyPluginPropertyInlineErrorMessageColor(property);
  }

  @Then("Enter BiqQueryMultiTable sink property reference name {string}")
  public void enterBigQueryMultiTableSinkPropertyReferenceName(String reference) {
    BQMTActions.enterReferenceName(PluginPropertyUtils.pluginProp(reference));
  }

  @Then("Enter BiqQueryMultiTable sink property temporary bucket name {string}")
  public void enterBigQueryMultiTableSinkPropertyTemporaryBucketName(String temporaryBucket) throws IOException {
    BQMTActions.enterTemporaryBucketName(PluginPropertyUtils.pluginProp(temporaryBucket));
  }

  @Then("Select BiqQueryMultiTable sink property update table schema as {string}")
  public void selectBigQueryMultiTableSinkPropertyUpdateTableSchemaAs(String updateTableSchema) {
    BQMTActions.selectUpdateTableSchema(updateTableSchema);
  }

  @Then("Enter BiqQueryMultiTable sink property split field {string}")
  public void enterBigQueryMultiTableSinkPropertySplitField(String splitField) {
    BQMTActions.enterSplitField(PluginPropertyUtils.pluginProp(splitField));
  }

  @Then("Enter BigQueryMultiTable sink property {string} as macro argument {string}")
  public void enterBigQueryMultiTableSinkPropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    enterPropertyAsMacroArgument(pluginProperty, macroArgument);
  }

  @Then("Enter BigQueryMultiTable sink cmek property {string} as macro argument {string} if cmek is enabled")
  public void enterBigQueryMultiTableSinkCmekPropertyAsMacroArgumentIfCmekIsEnabled(String pluginProperty,
                                                                                    String macroArgument) {
    String cmekBQ = PluginPropertyUtils.pluginProp("cmekBQ");
    if (cmekBQ != null) {
      enterPropertyAsMacroArgument(pluginProperty, macroArgument);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Enter BiqQueryMultiTable sink property encryption key name {string} if cmek is enabled")
  public void enterBiqQueryMultiTableSinkPropertyEncryptionKeyNameIfCmekIsEnabled(String cmek) throws IOException {
    String cmekBQ = PluginPropertyUtils.pluginProp(cmek);
    if (cmekBQ != null) {
      BQMTActions.enterBQMTCmekProperty(cmekBQ);
      BeforeActions.scenario.write("Entered encryption key name - " + cmekBQ);
    }
  }

  @Then("Validate records transferred to target BQMT tables {string} is equal to number of records from " +
    "source BQ table")
  public void validateRecordsTransferredToTargetBQMTTablesIsEqualToNumberOfRecordsFromSourceBQTable
    (String targetBQMTTables)
    throws IOException, InterruptedException {
    String[] targetBQMTs = PluginPropertyUtils.pluginProp(targetBQMTTables).split(",");
    int targetTotalRecordsCount = 0;
    for (String targetBQTable : targetBQMTs) {
      targetTotalRecordsCount += BigQueryClient.countBqQuery(targetBQTable);
    }
    BeforeActions.scenario.write("Number of records transferred to target tables" + targetTotalRecordsCount);
    int sourceBQRecordsCount = BigQueryClient.countBqQuery(TestSetupHooks.bqSourceTable);
    BeforeActions.scenario.write("Number of records from source table" + sourceBQRecordsCount);
    Assert.assertEquals(sourceBQRecordsCount, targetTotalRecordsCount);
  }

  @Then("Validate the cmek key {string} of target BQMT tables {string} if cmek is enabled")
  public void validateTheCmekKeyOfTargetBQMTTablesIfCmekIsEnabled(String cmek, String targetBQMTTables)
    throws IOException {
    String cmekBQ = PluginPropertyUtils.pluginProp(cmek);
    if (cmekBQ != null) {
      String[] targetBQMTs = targetBQMTTables.split(",");
      for (String targetBQTable : targetBQMTs) {
        Assert.assertTrue("Cmek key of target BigQuery table " + targetBQTable + " should be equal to " +
                            "cmek key provided in config file",
                          BigQueryClient.verifyCmekKey(targetBQTable, cmekBQ));
      }
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Validate records transferred to target BQMT tables {string}")
  public void validateRecordsTransferredToTargetBQMTTables(String bqmtTables) throws IOException, InterruptedException {
    int outCountRecords = 0;
    String[] targetTableRecordCount = PluginPropertyUtils.pluginProp(bqmtTables).split(",");
    for (String table : targetTableRecordCount) {
      int countTableRecords = countOfNoOfRecordsTransferredToBigQuery(table);
      outCountRecords = outCountRecords + countTableRecords;
    }
    Assert.assertTrue(outCountRecords > 0);
    Assert.assertEquals(outCountRecords, (long) this.recordIn());
  }

  private static int countOfNoOfRecordsTransferredToBigQuery(String tableName) throws IOException,
    InterruptedException {
    int countRecords = BigQueryClient.countBqQuery(tableName);
    BeforeActions.scenario.write("**********No of Records Transferred in table" + tableName + "*:" + countRecords);
    Assert.assertTrue(countRecords > 0);
    return countRecords;
  }
}
