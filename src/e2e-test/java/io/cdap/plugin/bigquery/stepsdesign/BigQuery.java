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
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.utils.CdapUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import stepsdesign.BeforeActions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_COLOR;
import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_ERROR_FOUND_VALIDATION;
import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_INCORRECT_TABLE;
import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_VALIDATION;

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

  @Then("Connect Source as {string} and sink as {string} to establish connection")
  public void connectSourceAndSinkToEstablishConnection(String source, String sink) throws InterruptedException {
    CdfStudioActions.connectSourceAndSink(source, sink);
  }

  @Then("Open BigQuery Properties")
  public void openBigQueryProperties() {
    CdfStudioActions.clickProperties("BigQuery");
  }

  @Then("Open BigQuery Target Properties")
  public void openBigQueryTargetProperties() {
    CdfStudioActions.clickProperties("BigQuery2");
  }

  @Then("Enter the BigQuery properties for table {string}")
  public void enterTheBigQueryPropertiesForTable(String tableName) throws InterruptedException, IOException {
    CdfBigQueryPropertiesActions.enterProjectId(CdapUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(CdapUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_Ref_" + UUID.randomUUID().toString());
    CdfBigQueryPropertiesActions.enterBigQueryDataset(CdapUtils.pluginProp("dataset"));
    CdfBigQueryPropertiesActions.enterBigQueryTable(CdapUtils.pluginProp(tableName));
  }

  @Then("Enter the BigQuery Properties for table {string} with filter {string}")
  public void enterTheBigQueryPropertiesForTableWithFilter(String tableName, String filter)
    throws IOException, InterruptedException {
    enterTheBigQueryPropertiesForTable(tableName);
    CdfBigQueryPropertiesActions.enterFilter(CdapUtils.pluginProp(filter));
  }

  @Then("Enter the BigQuery properties for table {string} with partitionStartDate {string} " +
    "and partitionEndDate {string}")
  public void enterTheBigQueryPropertiesForTableWithPartitionStartDateAndPartitionEndDate
    (String tableName, String partitionStartDate, String partitionEndDate) throws IOException, InterruptedException {
    enterTheBigQueryPropertiesForTable(tableName);
    CdfBigQueryPropertiesActions.enterPartitionStartDate(CdapUtils.pluginProp(partitionStartDate));
    CdfBigQueryPropertiesActions.enterPartitionEndDate(CdapUtils.pluginProp(partitionEndDate));
  }

  @Then("Enter the BigQuery Target Properties for table {string}")
  public void enterTheBigQueryTargetPropertiesForTable(String tableName) throws IOException, InterruptedException {
    enterTheBigQueryPropertiesForTable(tableName);
    CdfBigQueryPropertiesActions.clickUpdateTable();
    CdfBigQueryPropertiesActions.clickTruncatableSwitch();
  }

  @Then("Validate Bigquery properties")
  public void validateBigqueryProperties() {
    CdfGcsActions.clickValidateButton();
    String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_VALIDATION);
    String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  @Then("Capture output schema")
  public void captureOutputSchema() {
    CdfBigQueryPropertiesActions.getSchema();
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 10);
    wait.until(ExpectedConditions.numberOfElementsToBeMoreThan
      (By.xpath("//*[@placeholder=\"Field name\"]"), 2));
    SeleniumHelper.waitElementIsVisible(SeleniumDriver.getDriver().findElement(
      By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']")), 10L);
    List<WebElement> propertiesOutputSchemaElements = SeleniumDriver.getDriver().findElements(
      By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']"));
    for (WebElement element : propertiesOutputSchemaElements) {
      propertiesOutputSchema.add(element.getAttribute("value"));
    }
    Assert.assertTrue(propertiesOutputSchema.size() > 2);
  }

  @Then("Close the BigQuery properties")
  public void closeTheBigQueryProperties() {
    CdfStudioActions.clickCloseButton();
  }

  @Then("Enter the GCS properties")
  public void enterTheGCSProperties() throws InterruptedException, IOException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp("bqGcsBucketName"));
    CdfGcsActions.selectFormat("json");
    CdfStudioActions.clickValidateButton();
  }

  @Then("Close the GCS Properties")
  public void closeTheGCSProperties() {
    CdfStudioActions.clickCloseButton();
  }

  @Then("Add pipeline name")
  public void addPipelineName() throws InterruptedException {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("BigQuery_GCS" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.statusBanner);
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.statusBanner));
  }

  @Then("Preview and run the pipeline")
  public void previewAndRunThePipeline() {
    SeleniumHelper.waitAndClick(CdfStudioLocators.preview, 5L);
    CdfStudioLocators.runButton.click();
  }

  @Then("Verify the preview of pipeline is {string}")
  public void verifyThePreviewOfPipelineIs(String previewStatus) {
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 180);
    wait.until(ExpectedConditions.visibilityOf(CdfStudioLocators.statusBanner));
    Assert.assertTrue(CdfStudioLocators.statusBannerText.getText().contains(previewStatus));
    if (!previewStatus.equalsIgnoreCase("failed")) {
      wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.statusBanner));
    }
  }

  @Then("Click on PreviewData for BigQuery")
  public void clickOnPreviewDataForBigQuery() {
    CdfBigQueryPropertiesActions.clickPreviewData();
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

  @Then("Close the Preview and deploy the pipeline")
  public void clickOnPreviewAndDeployThePipeline() throws InterruptedException {
    SeleniumHelper.waitAndClick(CdfStudioLocators.closeButton, 5L);
    CdfStudioActions.previewSelect();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineDeploy, 2);
    CdfStudioActions.pipelineDeploy();
  }

  @Then("Save and Deploy BQ Pipeline")
  public void saveAndDeployPipeline() throws InterruptedException {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("BQ_TestPipeline" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.statusBanner);
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.statusBanner));
    CdfStudioActions.pipelineDeploy();
  }

  @Then("Run the Pipeline in Runtime")
  public void runThePipelineInRuntime() throws InterruptedException {
    CdfPipelineRunAction.runClick();
  }

  @Then("Wait till pipeline is in running state")
  public void waitTillPipelineIsInRunningState() throws InterruptedException {
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 300);
    wait.until(ExpectedConditions.or(
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
      ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
  }

  @Then("Verify the pipeline status is {string}")
  public void verifyThePipelineStatusIs(String status) {
    Assert.assertTrue(SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']"));
  }

  @Then("Open the Logs and capture raw logs")
  public void openTheLogsAndCaptureRawLogs() throws FileNotFoundException, InterruptedException {
    CdfPipelineRunAction.logsClick();
    rawLog = CdfPipelineRunAction.captureRawLogs();
    BeforeActions.scenario.write(rawLog);
    out.println(rawLog);
    out.close();
  }

  @Then("Validate the output record count")
  public void validateTheOutputRecordCount() {
    Assert.assertTrue(recordOut() > 0);
  }


  @Then("Get Count of no of records transferred to BigQuery in {string}")
  public void getCountOfNoOfRecordsTransferredToBigQueryIn(String tableName) throws IOException, InterruptedException {
    countRecords = GcpClient.countBqQuery(CdapUtils.pluginProp(tableName));
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    Assert.assertTrue(countRecords > 0);
  }

  @Then("Delete the table {string}")
  public void deleteTheTable(String table) {
    try {
      int existingRecords = GcpClient.countBqQuery(CdapUtils.pluginProp(table));
      if (existingRecords > 0) {
        GcpClient.dropBqQuery(CdapUtils.pluginProp(table));
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
    String projectId = (CdapUtils.pluginProp("projectId"));
    String datasetName = (CdapUtils.pluginProp("dataset"));
    String selectQuery = "SELECT count(*)  FROM `" + projectId + "." + datasetName + "." + CdapUtils.pluginProp
      (table) + "` WHERE " + CdapUtils.pluginProp(field);
    Optional<String> result = GcpClient.getSoleQueryResult(selectQuery);
    int count = result.map(Integer::parseInt).orElse(0);
    BeforeActions.scenario.write("number of records transferred with respect to filter:" + count);
    Assert.assertEquals(count, countRecords);
  }

  @Then("Validate partition date in output partitioned table {string}")
  public void validatePartitionDateInOutputPartitionedTable(String outputTable)
    throws IOException, InterruptedException {
    Optional<String> result = GcpClient
      .getSoleQueryResult("SELECT distinct  _PARTITIONDATE as pt FROM `" +
                            (CdapUtils.pluginProp("projectId")) + "." +
                            (CdapUtils.pluginProp("dataset")) + "." +
                            CdapUtils.pluginProp(outputTable) +
                            "` WHERE _PARTITION_LOAD_TIME IS Not NULL ORDER BY _PARTITIONDATE DESC ");
    String outputDate = StringUtils.EMPTY;
    if (result.isPresent()) {
      outputDate = result.get();
    }
    BeforeActions.scenario.write("partitioned date in output record:" + outputDate);
    Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd").format(new Date()), outputDate);
  }

  @Then("Validate the records are not created in output table {string}")
  public void validateTheRecordsAreNotCreatedInOutputTable(String table) throws IOException, InterruptedException {
    countRecords = GcpClient.countBqQuery(CdapUtils.pluginProp(table));
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    Assert.assertEquals(0, countRecords);
  }

  @Then("Validate partitioning is not done on the output table {string}")
  public void validatePartioningIsNotDoneOnTheOutputTable(String table) throws IOException, InterruptedException {
    try {
      GcpClient.getSoleQueryResult("SELECT distinct  _PARTITIONDATE as pt FROM `" +
                                                        (CdapUtils.pluginProp("projectId"))
                                                        + "." + (CdapUtils.pluginProp("dataset")) + "." +
                                                        CdapUtils.pluginProp(table)
                                                        + "` WHERE _PARTITION_LOAD_TIME IS Not NULL ");
    } catch (Exception e) {
      String partitionException = e.toString();
      Assert.assertTrue(partitionException.contains("Unrecognized name: _PARTITION_LOAD_TIME"));
      BeforeActions.scenario.write("Partition Not Created" + partitionException);
    }
  }

  @Then("Enter the BigQuery Properties with blank property {string}")
  public void enterTheBigQueryPropertiesWithBlankProperty(String property) throws IOException {
    if (property.equalsIgnoreCase("referenceName")) {
      CdfBigQueryPropertiesActions.enterBigQueryDataset(CdapUtils.pluginProp("dataset"));
      CdfBigQueryPropertiesActions.enterBigQueryTable(CdapUtils.pluginProp("bqTableName"));
    } else if (property.equalsIgnoreCase("dataset")) {
      CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_" + UUID.randomUUID().toString());
      CdfBigQueryPropertiesActions.enterBigQueryTable(CdapUtils.pluginProp("bqTableName"));
    } else if (property.equalsIgnoreCase("table")) {
      CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_" + UUID.randomUUID().toString());
      CdfBigQueryPropertiesActions.enterBigQueryDataset(CdapUtils.pluginProp("dataset"));
    }
  }

  @Then("Validate mandatory property error for {string}")
  public void validateMandatoryPropertyErrorFor(String property) {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton, 5L);
    CdapUtils.validateMandatoryPropertyError(property);
  }

  @Then("Enter the BigQuery Properties with incorrect property {string} value {string}")
  public void enterTheBigQueryPropertiesWithIncorrectProperty(String property, String value)
    throws IOException, InterruptedException {
    enterTheBigQueryPropertiesForTable("bqTableName");
    if (property.equalsIgnoreCase("dataset")) {
      SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.bigQueryDataSet, CdapUtils.pluginProp(value));
    } else if (property.equalsIgnoreCase("table")) {
      SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.bigQueryTable, CdapUtils.pluginProp(value));
    } else if (property.equalsIgnoreCase("datasetProject")) {
      SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.datasetProjectID, CdapUtils.pluginProp(value));
    }
  }

  @Then("Validate incorrect property error for table {string} value {string}")
  public void validateIncorrectPropertyErrorFor(String property, String value) {
    CdfBigQueryPropertiesActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfBigQueryPropertiesLocators.getSchemaButton, 5L);
    String tableFullName = StringUtils.EMPTY;
    if (property.equalsIgnoreCase("dataset")) {
      tableFullName = CdapUtils.pluginProp("projectId") + ":" + CdapUtils.pluginProp(value)
        + "." + CdapUtils.pluginProp("bqTableName");
    } else if (property.equalsIgnoreCase("table")) {
      tableFullName = CdapUtils.pluginProp("projectId") + ":" + CdapUtils.pluginProp("dataset")
        + "." + CdapUtils.pluginProp(value);
    } else if (property.equalsIgnoreCase("datasetProject")) {
      tableFullName = CdapUtils.pluginProp(value) + ":" + CdapUtils.pluginProp("dataset")
        + "." + CdapUtils.pluginProp("bqTableName");
    }
    String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_INCORRECT_TABLE)
      .replaceAll("TABLENAME", tableFullName);
    String actualErrorMessage = CdapUtils.findPropertyErrorElement("table").getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = CdapUtils.getErrorColor(CdapUtils.findPropertyErrorElement("table"));
    String expectedColor = CdapUtils.errorProp(ERROR_MSG_COLOR);
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Enter the BigQuery Properties with incorrect projectId format {string} value {string}")
  public void enterTheBigQueryPropertiesWithIncorrectProjectIdFormatValue(String field, String value)
    throws IOException, InterruptedException {
    enterTheBigQueryPropertiesForTable("bqTableName");
    if (field.equalsIgnoreCase("datasetProject")) {
      SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.datasetProjectID, CdapUtils.pluginProp(value));
    } else if (field.equalsIgnoreCase("project")) {
      SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.datasetProjectID, "");
      SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.projectID, CdapUtils.pluginProp(value));
    }
  }

  @Then("Verify plugin properties validation fails with error")
  public void verifyPluginPropertiesValidationFailsWithError() {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton, 5L);
    String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_ERROR_FOUND_VALIDATION);
    String actualErrorMessage = CdfStudioLocators.pluginValidationErrorMsg.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }
}
