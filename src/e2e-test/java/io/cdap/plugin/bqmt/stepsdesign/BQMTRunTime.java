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
package io.cdap.plugin.bqmt.stepsdesign;

import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.bqmt.actions.CdfBQMTActions;
import io.cdap.plugin.bqmt.locators.CdfBQMTLocators;
import io.cdap.plugin.utils.CdapUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;

import static io.cdap.plugin.utils.GCConstants.DELIMITED;
import static io.cdap.plugin.utils.GCConstants.TABLE_DEL_MSG;

/**
 * BQMT RunTime test cases.
 */
public class BQMTRunTime implements CdfHelper {

  @Given("Open Datafusion Project")
  public void openDatafusionProject() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source selected is GCS bucket")
  public void sourceSelectedIsGCSBucket() throws InterruptedException {
    CdfStudioActions.selectGCS();
  }

  @Then("Link GCS to {string} to establish connection")
  public void linkGCSToToEstablishConnection(String pluginName) {
    waitForSinkOnCanvas(pluginName);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromGCS, linkSinkPlugin(pluginName));
  }

  @Then("Enter the source GCS Properties with format {string} GCS bucket {string}")
  public void enterTheSourceGCSPropertiesWithFormatGCSBucket(String formatType, String bucket)
    throws IOException, InterruptedException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
    gcsProperties(CdapUtils.pluginProp(formatType));
  }

  @Then("Enter the GCS format with {string} GCS bucket")
  public void enterTheGCSFormatWithGCSBucket(String format) throws IOException, InterruptedException {
    CdfGcsActions.selectFormat(format);
    String expectedString = DELIMITED;
    String actualString = SeleniumHelper.readParameters(format);
    if (expectedString.equalsIgnoreCase(actualString)) {
      CdfGcsActions.delimiter();
    }
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();
  }

  @Then("Verify the get schema status")
  public void verifyTheGetSchemaStatus() throws InterruptedException {
    Thread.sleep(10000); //TODO remove this
    CdfGcsActions.clickValidateButton();
    CdfPipelineRunAction.schemaStatusValidation();
  }

  @Then("Validate the Schema")
  public void validateTheSchema() {
    CdfGcsActions.validateSchema();
  }

  @Then("Verify the Connector status")
  public void verifyTheConnectorStatus() {
    CdfGcsActions.validateSuccessMessage();
  }

  @Then("Close the Properties of GCS")
  public void closeThePropertiesOfGCS() {
    CdfGcsActions.closeButton();
  }

  @Then("Enter the BQMT Properties")
  public void enterTheBQMTProperties() throws IOException {
    CdfBQMTActions.bqmtProperties();
    SeleniumHelper.waitElementIsVisible(CdfBQMTLocators.bqmtPropertyHeader);
    CdfBQMTActions.enterReferenceName();
    CdfBQMTActions.enterDataset();
    CdfBQMTActions.truncateTable();
    CdfBQMTActions.temporaryBucketName();
    CdfBQMTActions.allowFlexibleSchemaInOutput();
    CdfBQMTActions.setTableSchemaTrue();
    CdfGcsActions.clickValidateButton();
  }

  @Then("Save and Deploy Pipeline of GCS to BQMT")
  public void saveAndDeployPipelineOfGCSToBQMT() throws InterruptedException {
    saveAndDeployPipeline();
  }

  @Then("Run the Pipeline in Runtime  to transfer record")
  public void runThePipelineInRuntimeToTransferRecord() throws InterruptedException {
    runThePipelineInRuntime();
  }

  @Then("Wait till pipeline run")
  public void waitTillPipe() {
    waitTillPipelineToComplete();
  }

  @Then("Verify the pipeline status is {string} for the pipeline")
  public void verifyThePipelineStatus(String status) {
    verifyThePipelineStatusIsForTheCurrentPipeline(status);
  }

  @Then("Open and capture Logs")
  public void openAndCaptureLogs() {
    captureLogs();
  }

  @Then("Get Count of no of records transferred to BigQuery {string} {string} {string}")
  public void getCountOfNoOfRecordsTransferredToBigQuery(String table1, String table2, String table3)
    throws IOException, InterruptedException {
    //TODO verify in and out count from UI , both should be greater than zero, and equal, remove this table records count
    int countTable1 = countOfNoOfRecordsTransferredToBigQueryIn(table1);
    int countTable2 = countOfNoOfRecordsTransferredToBigQueryIn(table2);
    int countTable3 = countOfNoOfRecordsTransferredToBigQueryIn(table3);
    int countRecords = countTable1 + countTable2 + countTable3;
    Assert.assertTrue(countRecords > 0);
  }

  @Then("Delete the BQMT table {string}")
  public void deleteTheBQMTTable(String table) {
  }

  @When("Target is {string}")
  public void targetIs(String target) {
    CdfStudioActions.clickSink();
    selectSinkPlugin(target);
  }
}
