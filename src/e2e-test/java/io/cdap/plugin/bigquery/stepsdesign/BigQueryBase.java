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
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PageHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.openqa.selenium.By;
import stepsdesign.BeforeActions;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

/**
 * BigQuery related common stepDesigns.
 */
public class BigQueryBase implements CdfHelper {

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

  @Then("Enter BiqQuery property encryption key name {string} if cmek is enabled")
  public void enterBiqQueryPropertyEncryptionKeyNameIfCmekIsEnabled(String cmek) throws IOException {
    String cmekBQ =  PluginPropertyUtils.pluginProp(cmek);
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
    String cmekBQ =  PluginPropertyUtils.pluginProp(cmek);
    if (cmekBQ != null) {
      Assert.assertTrue("Cmek key of target BigQuery table should be equal to " +
                            "cmek key provided in config file",
                          BigQueryClient.verifyCmekKey(TestSetupHooks.bqTargetTable, cmekBQ));
    } else {
      BeforeActions.scenario.write("CMEK not enabled");
    }
  }

  @Then("Validate plugin properties")
  public void validatePluginProperties() {
    CdfStudioActions.clickValidateButton();
  }

  @Then("Wait for studio service error")
  public void waitForStudioServiceError() {
    WaitHelper.waitForElementToBeDisplayed(
      SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy='configuration-group']//h2[text()='Errors']")));
  }

  @Then("Navigate to System admin page")
  public void navigateToSystemAdminPage() {
    ElementHelper.clickOnElement(SeleniumDriver.getDriver().findElement(By.xpath("//a[@data-cy='System Admin']")));
    PageHelper.acceptAlertIfPresent();
    WaitHelper.waitForPageToLoad();
  }

  @Then("Capture Pipeline studio service logs")
  public void capturePipelineStudioServiceLogs() throws FileNotFoundException {
    ElementHelper.clickOnElement(SeleniumDriver.getDriver().findElement
      (By.xpath("//table//td/span[text()='Pipeline Studio']/parent::td/following-sibling::td/a[text()='View Logs']")));
    String message = "---------------------------------------------------------------------------------------" +
      "PIPELINE STUDIO LOGS" +
      "---------------------------------------------------------------------------------------";
    writeLogsToFile(message, captureSystemAdminRawLogs());
  }

  @Then("Capture App Fabric logs")
  public void captureAppFabricLogs() throws FileNotFoundException {
    ElementHelper.clickOnElement(SeleniumDriver.getDriver().findElement
      (By.xpath("//table//td/span[text()='App Fabric']/parent::td/following-sibling::td/a[text()='View Logs']")));
    String message = "---------------------------------------------------------------------------------------" +
      "APP FABRIC LOGS" +
      "---------------------------------------------------------------------------------------";
    writeLogsToFile(message, captureSystemAdminRawLogs());
  }

  private String captureSystemAdminRawLogs() {
    String parent = SeleniumDriver.getDriver().getWindowHandle();
    ArrayList<String> tabs2 = new ArrayList(SeleniumDriver.getDriver().getWindowHandles());
    SeleniumDriver.getDriver().switchTo().window((String) tabs2.get(1));
    String logs = SeleniumDriver.getDriver().findElement(By.xpath("/html/body/pre")).getText();
    Assert.assertNotNull(logs);
    SeleniumDriver.getDriver().close();
    SeleniumDriver.getDriver().switchTo().window(parent);
    return logs;
  }

  private void writeLogsToFile(String message, String rawLogs) throws FileNotFoundException {
    BeforeActions.scenario.write(message);
    BeforeActions.scenario.write(rawLogs);
    PrintWriter out;
    if (BeforeActions.file.exists()) {
      out = new PrintWriter(new FileOutputStream(BeforeActions.file, true));
    } else {
      out = new PrintWriter(BeforeActions.file);
    }
    out.println(message);
    out.println(rawLogs);
    out.close();
  }
}
