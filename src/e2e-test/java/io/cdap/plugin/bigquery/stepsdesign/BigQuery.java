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
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfGCSLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.utils.CdapUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import stepsdesign.BeforeActions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.cdap.e2e.utils.ConstantsUtil.AUTOMATION_TEST;
import static io.cdap.e2e.utils.ConstantsUtil.DATASET;
import static io.cdap.e2e.utils.ConstantsUtil.ONE;
import static io.cdap.e2e.utils.ConstantsUtil.PROJECT_ID;
import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_DATASET;
import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_REF;
import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_TABLENAME;

/**
 * BigQuery related stepDesigns.
 */
public class BigQuery implements CdfHelper {
  static PrintWriter out;
  static String rawLog;
  static int countRecords;
  List<String> propertiesOutputSchema = new ArrayList<String>();

  static {
    try {
      out = new PrintWriter(BeforeActions.myObj);
    } catch (FileNotFoundException e) {
      BeforeActions.scenario.write(e.toString());
    }
  }

  GcpClient gcpClient = new GcpClient();

  @Given("Open Datafusion Project to configure pipeline")
  public void openDatafusionProjectToConfigurePipeline() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source is BigQuery")
  public void sourceIsBigQuery() throws InterruptedException {
    CdfStudioActions.selectBQ();
  }

  @When("Sink is GCS")
  public void sinkIsGCS() {
    CdfStudioActions.sinkGcs();
  }

  @When("Target is BigQuery")
  public void targetIsBigQuery() {
    CdfStudioActions.sinkBigQuery();
  }

  @Then("Connect Source as BigQuery and sink as GCS to establish connection")
  public void connectSourceAndSinkToEstablishConnection() throws InterruptedException {
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.toGCS);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromBigQuery, CdfStudioLocators.toGCS);
  }

  @Then("Close the BigQuery properties")
  public void closeTheBigQueryProperties() {
    CdfBigQueryPropertiesActions.closeButton();
  }

  @Then("Enter the GCS properties")
  public void enterTheGCSProperties() throws InterruptedException, IOException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp("bqGcsBucketName"));
    CdfGcsActions.selectFormat("json");
    SeleniumHelper.waitElementIsVisible(CdfGCSLocators.validateBtn);
    CdfGcsActions.clickValidateButton();
  }

  @Then("Close the GCS Properties")
  public void closeTheGCSProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Add pipeline name")
  public void addPipelineName() throws InterruptedException {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("BigQuery_GCS" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineSaveSuccessBanner);
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.pipelineSaveSuccessBanner));
  }

  @Then("Click the preview")
  public void clickThePreview() {
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.preview, 5);
    CdfStudioLocators.preview.click();
  }

  @Then("Close the Preview and deploy the pipeline")
  public void clickOnPreviewAndDeployThePipeline() throws InterruptedException {
    SeleniumHelper.waitElementIsVisible(CdfBigQueryPropertiesLocators.closeButton, 180);
    CdfBigQueryPropertiesActions.closeButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.preview, 50);
    CdfStudioActions.previewSelect();
    CdfStudioActions.pipelineDeploy();
  }

  @Then("Open the Logs and capture raw logs")
  public void openTheLogsAndCaptureRawLogs() throws FileNotFoundException, InterruptedException {
    CdfPipelineRunAction.logsClick();
    rawLog = CdfPipelineRunAction.captureRawLogs();
    BeforeActions.scenario.write(rawLog);
    out.println(rawLog);
    out.close();
  }

  @Then("Run and Preview Data")
  public void runAndPreviewData() throws InterruptedException {
    CdfStudioActions.runAndPreviewForData();
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.previewSuccessBanner));
    CdfBigQueryPropertiesActions.clickPreviewData();
  }

  @Then("Enter the BigQuery Properties for table {string} for source")
  public void enterTheBigQueryPropertiesForTableForSource(String tableName) throws IOException, InterruptedException {
    CdfBigQueryPropertiesActions.enterProjectId(CdapUtils.pluginProp("Project-ID"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(CdapUtils.pluginProp("Project-ID"));
    CdfBigQueryPropertiesActions.enterReferenceName();
    CdfBigQueryPropertiesActions.enterDataset(CdapUtils.pluginProp("Data-Set"));
    CdfBigQueryPropertiesActions.enterTable(CdapUtils.pluginProp(tableName));
    CdfBigQueryPropertiesActions.enableQueryingViews();
    CdfBigQueryPropertiesActions.viewMaterializationProject(
      CdapUtils.pluginProp("bqViewmaterializationproject"));
    CdfBigQueryPropertiesActions.viewMaterializationDataset(
      CdapUtils.pluginProp("bqViewmaterializationdataset"));
    CdfBigQueryPropertiesActions.getSchema();
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.numberOfElementsToBeMoreThan
      (By.xpath("//*[@placeholder=\"Field name\"]"), 2));
    Assert.assertEquals(true, CdapUtils.schemaValidation());
    CdfGcsActions.clickValidateButton();
  }

  @Then("Run the Pipeline in Runtime")
  public void runThePipelineInRuntime() throws InterruptedException {
    CdfPipelineRunAction.runClick();
  }

  @Then("Wait till pipeline is in running state")
  public void waitTillPipelineIsInRunningState() throws InterruptedException {
    Boolean bool = true;
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 250);
    wait.until(ExpectedConditions.or(
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
  }

  @Then("Verify the pipeline status is {string}")
  public void verifyThePipelineStatusIs(String status) {
    boolean webelement = false;
    webelement = SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']");
    Assert.assertTrue(webelement);
  }

  @Then("Click on Advance logs and validate the success message")
  public void clickOnAdvanceLogsAndValidateTheSuccessMessage() {
    CdfLogActions.goToAdvanceLogs();
    CdfLogActions.validateSucceeded();
  }

  @Then("Verify mandatory Fields")
  public void verifyMandatoryFields() {
    CdfBigQueryPropertiesActions.clickValidateButton();
    Assert.assertEquals(CdapUtils.errorProp(ERROR_MSG_REF),
                        CdfBigQueryPropertiesLocators.bigQueryReferenceNameError.getText());
    Assert.assertEquals(CdapUtils.errorProp(ERROR_MSG_DATASET),
                        CdfBigQueryPropertiesLocators.bigQueryDatasetError.getText());
    Assert.assertEquals(CdapUtils.errorProp(ERROR_MSG_TABLENAME),
                        CdfBigQueryPropertiesLocators.bigQueryTableError.getText());
  }

  @Then("Open Bigquery Properties")
  public void openBigqueryProperties() {
    CdfStudioActions.clickBigQueryProperties();
  }

  @Then("Enter the BigQuery Properties for filter on table {string} for source")
  public void enterTheBigQueryPropertiesForFilterOnForSource(String tableName) throws IOException {
    CdfBigQueryPropertiesActions.enterProjectId(CdapUtils.pluginProp("Project-ID"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(CdapUtils.pluginProp("Project-ID"));
    CdfBigQueryPropertiesActions.enterReferenceName();
    CdfBigQueryPropertiesActions.enterDataset(CdapUtils.pluginProp("Data-Set"));
    CdfBigQueryPropertiesActions.enterTable(CdapUtils.pluginProp(tableName));
    CdfBigQueryPropertiesActions.enterFilter(CdapUtils.pluginProp("bqFilter"));
    CdfBigQueryPropertiesActions.getSchema();
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.numberOfElementsToBeMoreThan
      (By.xpath("//*[@placeholder=\"Field name\"]"), 2));
    Assert.assertEquals(true, CdapUtils.schemaValidation());
    CdfGcsActions.clickValidateButton();
    CdfGcsActions.clickValidateButton();
  }

  @Then("Validate the output record count")
  public void validateTheOutputRecordCount() {
    Assert.assertTrue(recordOut() > 0);
  }

  @Then("Enter the BigQuery Properties for table {string}")
  public void enterTheBigQueryPropertiesForTable(String tableName) throws InterruptedException, IOException {
    CdfBigQueryPropertiesActions.enterBigQueryProperties(CdapUtils.pluginProp(tableName));
  }

  @Then("Enter the partitionStartDate {string} and  partitionEndDate {string}")
  public void enterThePartitionStartDateAndPartitionEndDate(String startdate, String enddate) throws IOException {
    CdfBigQueryPropertiesActions.enterPartitionStartDate(
      CdapUtils.pluginProp(startdate));
    CdfBigQueryPropertiesActions.enterPartitionEndDate(CdapUtils.pluginProp(enddate));
  }

  @Then("Connect Source as BigQuery and sink as BigQuery to establish connection")
  public void connectSourceAsBigQueryAndSinkAsBigQueryToEstablishConnection() throws InterruptedException {
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.toBigQuery2);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromBigQuery, CdfStudioLocators.toBigQuery2);
  }

  @Then("Open BigqueryTarget Properties")
  public void openBigqueryTargetProperties() {
    CdfStudioLocators.bigQueryProperties2.click();
  }

  @Then("Enter the BigQueryTarget Properties for table {string}")
  public void enterTheBigQueryTargetPropertiesForTable(String arg0) throws IOException, InterruptedException {
    CdfBigQueryPropertiesLocators.bigQueryReferenceName.sendKeys(AUTOMATION_TEST);
    CdfBigQueryPropertiesLocators.datasetProjectId.sendKeys(CdapUtils.pluginProp("Project-ID"));
    SeleniumHelper.replaceElementValue
      (CdfBigQueryPropertiesLocators.projectID, (CdapUtils.pluginProp("Project-ID")));
    CdfBigQueryPropertiesLocators.bigQueryDataSet.sendKeys((CdapUtils.pluginProp("Data-Set")));
    CdfBigQueryPropertiesLocators.bigQueryTable.sendKeys(CdapUtils.pluginProp("tableDemo"));
    CdfBigQueryPropertiesLocators.updateTable.click();
    CdfBigQueryPropertiesLocators.truncatableSwitch.click();
    CdfBigQueryPropertiesLocators.validateBttn.click();
    SeleniumHelper.waitElementIsVisible(CdfBigQueryPropertiesLocators.textSuccess, ONE);
  }

  @Then("Close the BigQueryTarget Properties")
  public void closeTheBigQueryTargetProperties() {
    CdfBigQueryPropertiesActions.closeButton();
  }

  @Then("Verify the preview of pipeline is {string}")
  public void verifyThePreviewOfPipelineIs(String previewStatus) {
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 180);
    wait.until(ExpectedConditions.visibilityOfElementLocated(
      By.xpath("//*[@data-cy='valium-banner-hydrator']//span[contains(text(),'" + previewStatus + "')]")));
    if (!previewStatus.equalsIgnoreCase("failed")) {
      wait.until(ExpectedConditions.invisibilityOfElementLocated(By.xpath("//*[@data-cy='valium-banner-hydrator']")));
    }
  }

  @Then("Get Count of no of records transferred to BigQuery in {string}")
  public void getCountOfNoOfRecordsTransferredToBigQueryIn(String tableName) throws IOException, InterruptedException {
    countRecords = gcpClient.countBqQuery(CdapUtils.pluginProp(tableName));
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    Assert.assertTrue(countRecords > 0);
  }

  @Then("Delete the table {string}")
  public void deleteTheTable(String table) {
    try {
      int existingRecords = gcpClient.countBqQuery(CdapUtils.pluginProp(table));
      if (existingRecords > 0) {
        gcpClient.dropBqQuery(CdapUtils.pluginProp(table));
        BeforeActions.scenario.write("Table Deleted Successfully");
      }
    } catch (Exception e) {
      BeforeActions.scenario.write(e.toString());
    }
  }

  @Then("Validate successMessage is displayed when pipeline is succeeded")
  public void validateSuccessMessageIsDisplayedWhenPipelineIsSucceeded() {
    CdfLogActions.validateSucceeded();
  }

  @Then("Validate record transferred from table {string} on the basis of filter {string} is equal to the " +
    "total no of records")
  public void validateRecordTransferredFromOnTheBasisOfIsEqualToTheTotalNoOfRecords
    (String table, String field) throws IOException, InterruptedException {
    String projectId = (CdapUtils.pluginProp("Project-ID"));
    String datasetName = (CdapUtils.pluginProp("Data-Set"));
    String selectQuery = "SELECT count(*)  FROM `" + projectId + "." + datasetName + "." + CdapUtils.pluginProp
      (table) + "` WHERE " +
      CdapUtils.pluginProp(field);
    int count = GcpClient.executeQuery(selectQuery);
    BeforeActions.scenario.write("number of records transferred with respect to filter:"
                                   + count);
    Assert.assertEquals(count, countRecords);
  }

  @Then("Capture output schema")
  public void captureOutputSchema() {
    SeleniumDriver.getDriver().findElement(By.xpath("//button[@data-cy='plugin-properties-validate-btn']")).click();
    SeleniumHelper.waitElementIsVisible(SeleniumDriver.getDriver().findElement(
      By.xpath("//*[@data-cy='plugin-validation-success-msg']")), 10L);
    SeleniumHelper.waitElementIsVisible(SeleniumDriver.getDriver().findElement(
      By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']")), 10L);
    List<WebElement> propertiesOutputSchemaElements = SeleniumDriver.getDriver().findElements(
      By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']"));
    for (WebElement element : propertiesOutputSchemaElements) {
      propertiesOutputSchema.add(element.getAttribute("value"));
    }
  }

  @Then("Verify Preview output schema matches the outputSchema captured in properties")
  public void verifyPreviewOutputSchemaMatchesTheOutputSchemaCapturedInProperties() {
    List<String> previewOutputSchema = new ArrayList<String>();
    List<WebElement> previewOutputSchemaElements = SeleniumDriver.getDriver().findElements(
      By.xpath("(//h2[text()='Output Records']/parent::div/div/div/div/div)[1]//div[text()!='']"));
    for (WebElement element : previewOutputSchemaElements) {
      previewOutputSchema.add(element.getAttribute("title"));
    }
    Assert.assertTrue(previewOutputSchema.equals(propertiesOutputSchema));
  }

  @Then("Validate Partition date of table- {string} is equal to output partitioned table {string}")
  public void validatePartitionDateOfTableIsEqualToOutputPartitionedTable(String inputTable, String ouputTable)
    throws IOException, InterruptedException {
    String inputDate = GcpClient.executeSelectQuery("SELECT distinct  _PARTITIONDATE as pt FROM `" +
                                                      (CdapUtils.pluginProp("Project-ID"))
                                                      + "." + (CdapUtils.pluginProp("Data-Set")) + "." +
                                                      CdapUtils.pluginProp(inputTable)
                                                      + "` WHERE _PARTITION_LOAD_TIME IS Not NULL ");

    String outputDate = GcpClient.executeSelectQuery("SELECT distinct  _PARTITIONDATE as pt FROM `" +
                                                       (CdapUtils.pluginProp("Project-ID")) + "."
                                                       + (CdapUtils.pluginProp("Data-Set")) + "." +
                                                       CdapUtils.pluginProp(ouputTable)
                                                       + "` WHERE _PARTITION_LOAD_TIME IS Not NULL ");
    BeforeActions.scenario.write("partioned start date in output record:"
                                   + outputDate);
    Assert.assertEquals(inputDate, outputDate);
  }

  @Then("Enter the BigQuery Properties with incorrect filter {string} " +
    "values in the fields on table {string} for source")
  public void enterTheBigQueryPropertiesWithIncorrectValuesInTheFiledsOnForSource
    (String invalidFilter, String tableName) throws IOException {
    CdfBigQueryPropertiesActions.enterProjectId(CdapUtils.pluginProp("Project-ID"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(CdapUtils.pluginProp("Project-ID"));
    CdfBigQueryPropertiesActions.enterReferenceName();
    CdfBigQueryPropertiesActions.enterDataset(CdapUtils.pluginProp("Data-Set"));
    CdfBigQueryPropertiesActions.enterTable(CdapUtils.pluginProp(tableName));
    CdfBigQueryPropertiesActions.enterFilter(CdapUtils.pluginProp("bqinvalidFilter"));
    CdfBigQueryPropertiesActions.getSchema();
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 20000);
    wait.until(ExpectedConditions.numberOfElementsToBeMoreThan
      (By.xpath("//*[@placeholder=\"Field name\"]"), 2));
    Assert.assertEquals(true, CdapUtils.schemaValidation());
    CdfGcsActions.clickValidateButton();
  }

  @Then("Validate the records are not created in output table {string}")
  public void validateTheRecordsAreNotCreatedInOutputTable(String table) throws IOException, InterruptedException {
    countRecords = gcpClient.countBqQuery(CdapUtils.pluginProp(table));
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    Assert.assertTrue(countRecords == 0);
  }

  @Then("Validate partitioning is not done on the output table {string}")
  public void validatePartioningIsNotDoneOnTheOutputTable(String table) throws IOException,
    InterruptedException {
    try {
      String inputDate = GcpClient.executeSelectQuery("SELECT distinct  _PARTITIONDATE as pt FROM `" +
                                                        (CdapUtils.pluginProp("Project-ID"))
                                                        + "." + (CdapUtils.pluginProp("Data-Set")) + "." +
                                                        CdapUtils.pluginProp(table)
                                                        + "` WHERE _PARTITION_LOAD_TIME IS Not NULL ");
    } catch (Exception e) {
      String partitionException = e.toString();
      Assert.assertTrue(partitionException.toLowerCase().contains("Unrecognized name: _PARTITION_LOAD_TIME"));
      BeforeActions.scenario.write("Partition Not Created" + partitionException);
    }
  }

  @Then("Save and Deploy BQ Pipeline")
  public void saveAndDeployPipeline() throws InterruptedException {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("TestPipeline" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineSaveSuccessBanner);
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.pipelineSaveSuccessBanner));
    CdfStudioActions.pipelineDeploy();
  }

  @Then("Enter the BigQuery Properties for table {string} without Reference Name")
  public void enterTheBigQueryPropertiesForTableForField(String tableName, String blankReference) throws IOException {
    CdfBigQueryPropertiesActions.enterProjectId(CdapUtils.pluginProp("Project-ID"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(CdapUtils.pluginProp("Project-ID"));
    CdfBigQueryPropertiesActions.enterDataset(CdapUtils.pluginProp("Data-Set"));
    CdfBigQueryPropertiesActions.enterTable(CdapUtils.pluginProp(tableName));
    CdfGcsActions.clickValidateButton();
  }

  @Then("Enter the BigQuery Properties for table {string} without dataset field")
  public void enterTheBigQueryPropertiesForTableWhenDatasetValueIsNotProvided(String tableName) throws IOException {
    CdfBigQueryPropertiesActions.enterProjectId(CdapUtils.pluginProp("Project-ID"));
    CdfBigQueryPropertiesActions.enterReferenceName();
    CdfBigQueryPropertiesActions.enterTable(CdapUtils.pluginProp(tableName));
    CdfGcsActions.clickValidateButton();
  }

  @Then("Verify Reference Field throws error")
  public void verifyReferenceFieldThrowsError() {
    CdfBigQueryPropertiesActions.clickValidateButton();
    Assert.assertEquals(CdapUtils.errorProp(ERROR_MSG_REF),
                        CdfBigQueryPropertiesLocators.bigQueryReferenceNameError.getText());
  }

  @Then("Verify Dataset Field throws error")
  public void verifyDatasetFieldThrowsError() {
    CdfBigQueryPropertiesActions.clickValidateButton();
    Assert.assertEquals(CdapUtils.errorProp(ERROR_MSG_DATASET),
                        CdfBigQueryPropertiesLocators.bigQueryDatasetError.getText());
  }

  @Then("Verify Table Field throws error")
  public void verifyTableFieldThrowsError() {
    CdfBigQueryPropertiesActions.clickValidateButton();
    Assert.assertEquals(CdapUtils.errorProp(ERROR_MSG_TABLENAME),
                        CdfBigQueryPropertiesLocators.bigQueryTableError.getText());
  }

  @Then("Enter the BigQuery Properties for table {string} without table name field")
  public void enterTheBigQueryPropertiesForTableWhenTableValueIsNotProvided(String arg0) throws IOException {
    CdfBigQueryPropertiesActions.enterProjectId(CdapUtils.pluginProp("Project-ID"));
    CdfBigQueryPropertiesActions.enterReferenceName();
    CdfBigQueryPropertiesActions.enterDatasetProjectId(CdapUtils.pluginProp("Project-ID"));
    CdfGcsActions.clickValidateButton();
  }
}
