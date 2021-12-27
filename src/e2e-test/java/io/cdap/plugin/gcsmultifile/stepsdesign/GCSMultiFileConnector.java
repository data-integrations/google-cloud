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
package io.cdap.plugin.gcsmultifile.stepsdesign;

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
import io.cdap.plugin.gcsmultifile.actions.GCSMultiFileActions;
import io.cdap.plugin.gcsmultifile.locators.GCSMultiFileLocators;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cdap.plugin.utils.E2ETestUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import stepsdesign.BeforeActions;
import java.io.IOException;
import java.util.UUID;

/**
 * GCSMultiFile connector related design.
 */
public class GCSMultiFileConnector implements CdfHelper {

  public static String folderName;
  public static int inputCount;

  @Given("Open Datafusion Project to configure pipeline")
  public void openDatafusionProjectToConfigurePipeline() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source is BigQuery")
  public void sourceIsBigQuery() throws InterruptedException {
    CdfStudioActions.selectBQ();
  }

  @When("Source is GCS bucket")
  public void sourceIsGCSBucket() throws InterruptedException {
    CdfStudioActions.selectGCS();
  }

  @When("Target is GcsMultiFile")
  public void targetIsGcsMultifile() {
    GCSMultiFileActions.selectGcsMultifile();
  }

  @Then("Link Source and Sink to establish connection")
  public void linkSourceAndSinkToEstablishConnection() throws InterruptedException {
    SeleniumHelper.waitElementIsVisible(GCSMultiFileLocators.toGcsMultifile);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromBigQuery, GCSMultiFileLocators.toGcsMultifile);
  }

  @Then("Link GCS and GCSMultiFile to establish connection")
  public void linkGCSAndGCSMultiFileToEstablishConnection() throws InterruptedException {
    SeleniumHelper.waitElementIsVisible(GCSMultiFileLocators.toGcsMultifile);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromGCS, GCSMultiFileLocators.toGcsMultifile);
  }

  @Then("Enter the BigQuery Properties for table {string} amd dataset {string} for source")
  public void enterTheBigQueryPropertiesForTableForSource(String table, String dataset)
    throws IOException, InterruptedException {
    CdfStudioActions.clickProperties("BigQuery");
    CdfBigQueryPropertiesActions.enterProjectId(E2ETestUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(E2ETestUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName(E2ETestUtils.pluginProp("multiFileBQRefName"));
    CdfBigQueryPropertiesActions.enterBigQueryDataset(E2ETestUtils.pluginProp(dataset));
    CdfBigQueryPropertiesActions.enterBigQueryTable(E2ETestUtils.pluginProp(table));
    CdfBigQueryPropertiesActions.getSchema();
    SeleniumHelper.waitElementIsVisible(GCSMultiFileLocators.getSchemaLoadComplete);
  }

  @Then("Close the BigQuery Properties")
  public void closeTheBigQueryProperties() {
    CdfStudioActions.clickCloseButton();
  }

  @Then("Enter the Gcs MultiFile Properties for table {string} and format {string}")
  public void enterTheGcsMultifilePropertiesForTableAndFormat(String path, String formatType)
    throws IOException, InterruptedException {
    GCSMultiFileActions.gcsMultifileProperties();
    GCSMultiFileActions.enterReferenceName();
    GCSMultiFileActions.enterProjectId(E2ETestUtils.pluginProp("projectId"));
    GCSMultiFileActions.enterGcsMultifilepath(E2ETestUtils.pluginProp(path));
    GCSMultiFileActions.selectFormat(E2ETestUtils.pluginProp(formatType));
    GCSMultiFileActions.selectAllowFlexibleSchema();
  }

  @Then("Close Gcs MultiFile Properties")
  public void closeGcsMultifileProperties() {
    GCSMultiFileActions.closeGcsMultifile();
  }

  @Then("verify the schema in output")
  public void verifyTheSchemaInOutput() {
    CdfGcsActions.validateSchema();
  }

  @Then("Verify the pipeline status in each case")
  public void verifyThePipelineStatusInFailedCase() {
    WebElement status = SeleniumDriver.getDriver().
      findElement(By.xpath("//*[@data-cy='Succeeded' or @data-cy='Failed']"));
    String str = status.getText();
    Assert.assertEquals(str, "Succeeded");
  }

  @Then("Save and Deploy Pipeline")
  public void saveAndDeployPipeline() throws InterruptedException {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("TestPipeline" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    SeleniumHelper.waitElementIsVisible(GCSMultiFileLocators.pipelineSaveSuccessBanner);
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.invisibilityOf(GCSMultiFileLocators.pipelineSaveSuccessBanner));
    CdfStudioActions.pipelineDeploy();
  }

  @Then("Run the Pipeline in Runtime")
  public void runThePipelineInRuntime() throws InterruptedException {
    CdfPipelineRunAction.runClick();
  }

  @Then("Wait till pipeline is in running state")
  public void waitTillPipelineIsInRunningState() throws InterruptedException {
    Boolean bool = true;
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 1000000);
    wait.until(ExpectedConditions.or(ExpectedConditions.
                                       visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
                                     ExpectedConditions.
                                       visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
  }

  @Then("Verify the pipeline status is {string}")
  public void verifyThePipelineStatusIs(String status) {
    boolean webelement = false;
    webelement = SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']");
    Assert.assertTrue(webelement);
  }

  @Then("Open Logs")
  public void openLogs() {
    CdfPipelineRunAction.logsClick();
  }

  @Then("Validate successMessage is displayed")
  public void validateSuccessMessageIsDisplayed() {
    CdfLogActions.validateSucceeded();
  }

  @Then("Click on Advance logs and validate the success message")
  public void clickOnAdvanceLogsAndValidateTheSuccessMessage() {
    CdfLogActions.goToAdvanceLogs();
    CdfLogActions.validateSucceeded();
  }

  @Then("Close the GCS Properties")
  public void closeTheGCSProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Enter the GCS Properties with {string} GCS bucket")
  public void enterTheGCSPropertiesWithGCSBucket(String bucket) throws InterruptedException, IOException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.getGcsBucket(E2ETestUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat(E2ETestUtils.pluginProp("gcsCSVFileFormat"));
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(GCSMultiFileLocators.getSchemaLoadComplete);
  }

  @Then("Click on Source")
  public void clickOnSource() {
    GCSMultiFileActions.clickSource();
  }

  @Then("Verify Content Type Validation")
  public void verifyContentTypeValidation() {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = E2ETestUtils.errorProp("errorMessageContentType");
    String actualErrorMessage = E2ETestUtils.findPropertyErrorElement("contentType").getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  @Then("Enter the Gcs MultiFile Properties for table {string} and format {string} with ContentType {string}")
  public void enterTheGcsMultifilePropertiesForTableAndFormatWithContentType(String path, String formatType,
                                                                             String contentType)
    throws InterruptedException, IOException {
    enterTheGcsMultifilePropertiesForTableAndFormat(path, formatType);
    GCSMultiFileActions.selectContentType(E2ETestUtils.pluginProp(contentType));
  }

  @Then("Enter the Gcs MultiFile Properties for table {string}, format {string} with Codec {string}")
  public void enterTheGcsMultifilePropertiesForTableFormatWithCodec(String path, String formatType, String codecType)
    throws IOException, InterruptedException {
    enterTheGcsMultifilePropertiesForTableAndFormat(path, formatType);
    GCSMultiFileActions.selectCodec(E2ETestUtils.pluginProp(codecType));
  }

  @Then("Enter the GCSMultiFile properties with blank property {string}")
  public void enterTheGCSMultifilePropertiesWithBlankProperty(String property) throws IOException {
    GCSMultiFileActions.gcsMultifileProperties();
    if (property.equalsIgnoreCase("referenceName")) {
      GCSMultiFileActions.enterGcsMultifilepath(E2ETestUtils.pluginProp("multiFileGcsPath"));
    } else if (property.equalsIgnoreCase("path")) {
      GCSMultiFileActions.enterReferenceName();
    } else if (property.equalsIgnoreCase("splitField")) {
      GCSMultiFileActions.enterReferenceName();
      GCSMultiFileActions.enterGcsMultifilepath(E2ETestUtils.pluginProp("multiFileGcsPath"));
      SeleniumHelper.replaceElementValue(GCSMultiFileLocators.splitField, "");
    } else {
      Assert.fail("Invalid multiFile Mandatory Field " + property);
    }
  }

  @Then("Verify required property error for {string}")
  public void verifyRequiredPropertyErrorFor(String property) {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton);
    E2ETestUtils.validateMandatoryPropertyError(property);
  }

  @Then("Verify invalid path name error message is displayed for path {string}")
  public void verifyInvalidPathNameErrorMessageIsDisplayedForPath(String path) {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton);
    String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_GCS_INVALID_BUCKET_NAME)
      .replace("BUCKET_NAME", E2ETestUtils.pluginProp(path));
    String actualErrorMessage = E2ETestUtils.findPropertyErrorElement("path").getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement("path"));
    String expectedColor = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_COLOR);
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Enter the Gcs MultiFile Properties for table {string} and delimited format {string} and delimiter {string}")
  public void enterTheGcsMultiFilePropertiesForTableAndDelimitedFormatAndDelimiter(
    String path, String formatType, String delimiter)
    throws IOException, InterruptedException {
    enterTheGcsMultifilePropertiesForTableAndFormat(path, formatType);
    GCSMultiFileActions.enterDelimiter(E2ETestUtils.pluginProp(delimiter));
    CdfStudioActions.clickValidateButton();
  }

  @Given("Cloud Storage bucket should not exist in {string} with the name {string}")
  public void projectIdcloudStorageBucketShouldNotExistInWithTheName(String projectId, String bucketName) {
    E2ETestUtils.deleteBucket(E2ETestUtils.pluginProp(projectId), E2ETestUtils.pluginProp(bucketName));
  }

  @When("Target is selected as BigQuery")
  public void targetIsSelectedAsBigQuery() {
    CdfStudioActions.sinkBigQuery();
  }

  @Then("Link Source GCS and Sink bigquery to establish connection")
  public void linkSourceGCSAndSinkBigqueryToEstablishConnection() {
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.fromGCS);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromGCS, CdfStudioLocators.toBigQiery);
  }

  @Then("Verify the folder created in {string} with bucket name {string}")
  public void verifyTheFolderCreatedInWithBucketName(String projectID, String bucketName) {
    folderName = E2ETestUtils.listObjects(E2ETestUtils.pluginProp(projectID),
                                          E2ETestUtils.pluginProp(bucketName));
    Assert.assertTrue(folderName != null);
  }

  @Then("Enter the GCS Properties with {string} GCS bucket and skip header")
  public void enterTheGCSPropertiesWithGCSBucketAndSkipHeader(String bucketName)
    throws InterruptedException, IOException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.getGcsBucket(E2ETestUtils.pluginProp(bucketName) + "/" + folderName);
    CdfGcsActions.selectFormat(E2ETestUtils.pluginProp("gcsCSVFileFormat"));
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(GCSMultiFileLocators.getSchemaLoadComplete);
  }

  @Then("Get the count of the records transferred")
  public void getTheCountOfTheRecordsTransferred() {
    inputCount = recordOut();
    BeforeActions.scenario.write("Records Transferred :" + inputCount);
  }

  @Then("Get Count of no of records transferred to BigQuery from GCS {string}")
  public void getCountOfNoOfRecordsTransferredToBigQueryFromGCS(String tableName)
    throws IOException, InterruptedException {
    int countRecords;
    countRecords = GcpClient.countBqQuery(E2ETestUtils.pluginProp(tableName));
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    Assert.assertEquals(countRecords, inputCount);
  }

  @Then("Enter the BigQuery Properties for Sink {string}")
  public void enterTheBigQueryPropertiesForSink(String table) throws InterruptedException, IOException {
    CdfStudioLocators.bigQueryProperties.click();
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName(E2ETestUtils.pluginProp("multiFileBQRefName"));
    CdfBigQueryPropertiesActions.enterProjectId(E2ETestUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterBigQueryDataset(E2ETestUtils.pluginProp("dataset"));
    CdfBigQueryPropertiesActions.enterBigQueryTable(E2ETestUtils.pluginProp(table));
    CdfBigQueryPropertiesActions.clickUpdateTable();
    CdfBigQueryPropertiesActions.clickTruncatableSwitch();
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfBigQueryPropertiesLocators.textSuccess, 1L);
  }

  public static void validatePluginProperties() {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pluginValidationSuccessMsg, 10L);
    String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_VALIDATION);
    String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  @Then("Validate GCS MultiFile properties")
  public void validateGCSMultiFileProperties() {
    validatePluginProperties();
  }

  @Then("Validate BigQuery properties")
  public void validateBigQueryProperties() {
    validatePluginProperties();
  }

  @Then("Validate GCS properties")
  public void validateGCSProperties() {
    validatePluginProperties();
  }
}
