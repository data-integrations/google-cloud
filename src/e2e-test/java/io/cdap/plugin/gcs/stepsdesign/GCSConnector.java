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
package io.cdap.plugin.gcs.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfGCSLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
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
import java.util.List;
import java.util.UUID;

/**
 * GCSrefactor.
 */
public class GCSConnector implements CdfHelper {

  @Given("Open Datafusion Project to configure pipeline")
  public void openDatafusionProjectToConfigurePipeline() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source is GCS bucket")
  public void sourceIsGCSBucket() throws InterruptedException {
    CdfStudioActions.selectGCS();
  }

  @When("Target is BigQuery")
  public void targetIsBigQuery() {
    CdfStudioActions.sinkBigQuery();
  }

  @Then("Link Source and Sink to establish connection")
  public void linkSourceAndSinkToEstablishConnection() throws InterruptedException {
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.toBigQiery);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromGCS, CdfStudioLocators.toBigQiery);
  }

  @Then("Enter the GCS Properties with GCS bucket {string} and format {string}")
  public void enterTheGCSPropertiesWithGCSBucket(String bucket, String format) throws IOException,
    InterruptedException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterSamplesize();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat(CdapUtils.pluginProp(format));
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfGCSLocators.getSchemaLoadComplete, 30);
  }

  @Then("Enter the GCS Properties with GCS bucket {string} and format {string} by entering blank referenceName")
  public void enterTheGCSPropertiesByPuttingBlankValueInMandatoryFields(String bucket, String format)
    throws IOException, InterruptedException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat(CdapUtils.pluginProp(format));
  }

  @Then("verify the schema in output")
  public void verifyTheSchemaInOutput() {
    CdfGcsActions.validateSchema();
  }

  @Then("verify the datatype")
  public void verifyTheDatatype() {
    boolean flag = false;
    List<WebElement> elements = SeleniumDriver.getDriver().findElements(
      By.xpath("//*[@data-cy=\"select-undefined\"]/select"));
    for (WebElement datatype : elements) {
      String title = datatype.getAttribute("title");
      flag = title.equals("timestamp");
    }
    Assert.assertTrue(flag);
  }

  @Then("Enter the GCS Properties with GCS bucket {string} and format {string} by entering all fields")
  public void enterTheGCSPropertiesWithAllFieldsGCSBucket(String bucket, String format)
    throws IOException, InterruptedException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterSamplesize();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat(CdapUtils.pluginProp(format));
    CdfGcsActions.skipHeader();
    CdfGcsActions.enterMaxSplitSize(CdapUtils.pluginProp("gcsMaxsplitSize"));
    CdfGcsActions.enterMinSplitSize(CdapUtils.pluginProp("gcsMinsplitSize"));
    CdfGcsActions.enterRegexPath(CdapUtils.pluginProp("gcsRegexpath"));
    CdfGcsActions.enterPathField(CdapUtils.pluginProp("gcspathField"));
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfGCSLocators.getSchemaLoadComplete, 30);
  }

  @Then("Enter the GCS Properties with GCS bucket {string} , format {string} and fileEncoding {int}")
  public void enterTheGCSPropertiesWithUTFGCSBucket(String bucket, String format, int utf)
    throws InterruptedException, IOException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterSamplesize();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat(CdapUtils.pluginProp(format));
    CdfGcsActions.skipHeader();
    CdfGcsActions.selectFileEncoding(utf);
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfGCSLocators.getSchemaLoadComplete, 30);
  }

  @Then("Enter the BigQuery Properties for the table {string}")
  public void enterTheBigQueryProperties(String tableName) throws IOException, InterruptedException {
    CdfStudioActions.clickBigQueryProperties();
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName(CdapUtils.pluginProp("gcsBqRefName"));
    CdfBigQueryPropertiesActions.enterProjectId(CdapUtils.pluginProp("ProjectId"));
    CdfBigQueryPropertiesActions.enterBigQueryDataset(CdapUtils.pluginProp("dataset"));
    CdfBigQueryPropertiesActions.enterBigQueryTable(CdapUtils.pluginProp(tableName));
    CdfBigQueryPropertiesActions.clickUpdateTable();
    CdfBigQueryPropertiesActions.clickTruncatableSwitch();
    CdfBigQueryPropertiesActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfBigQueryPropertiesLocators.textSuccess, 1L);
  }

  @Then("Save and Deploy Pipeline")
  public void saveAndDeployPipeline() throws InterruptedException {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("TestPipeline" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineSaveSuccessBanner);
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.invisibilityOf(CdfStudioLocators.pipelineSaveSuccessBanner));
    CdfStudioActions.pipelineDeploy();
  }

  @Then("Run the Pipeline in Runtime")
  public void runThePipelineInRuntime() throws InterruptedException {
    CdfPipelineRunAction.runClick();
  }

  @Then("Wait till pipeline is in running state")
  public void waitTillPipelineIsInRunningState() throws InterruptedException {
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 180);
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
  public void openLogs() throws FileNotFoundException, InterruptedException {
    CdfPipelineRunAction.logsClick();
    BeforeActions.scenario.write(CdfPipelineRunAction.captureRawLogs());
    PrintWriter out = new PrintWriter(BeforeActions.myObj);
    out.println(CdfPipelineRunAction.captureRawLogs());
    out.close();
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

  @Then("Verify reference name is mandatory")
  public void verifyReferenceNameValidation() {
    CdfGcsActions.clickValidateButton();
    String expectedErrorMessage = CdapUtils.errorProp("errorMessageReference");
    String actualErrorMessage = CdfGCSLocators.referenceError.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    CdfGcsActions.getReferenceErrorColor();
  }

  @Then("Verify path is mandatory")
  public void verifyPathValidation() {
    CdfGcsActions.clickValidateButton();
    String expectedErrorMessage = CdapUtils.errorProp("errorMessagePath");
    String actualErrorMessage = CdfGCSLocators.pathError.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    CdfGcsActions.getPathErrorColor();
  }

  @Then("Close the GCS Properties")
  public void closeTheGCSProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Close the BigQuery Properties")
  public void closeTheBigQueryProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Enter the GCS Properties with format {string} by entering blank path")
  public void enterTheGCSPropertiesWithGCSBucketAndFormatByEnteringBlankPath(String format)
    throws InterruptedException, IOException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterSamplesize();
    CdfGcsActions.selectFormat(CdapUtils.pluginProp(format));
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();
  }

  @Then("Enter the GCS Properties with GCS bucket {string} and format {string} using OverrideDatatype")
  public void enterTheGCSPropertiesWithGCSBucketAndFormatUsingOverrideDatatype(String bucket, String format) throws
    IOException, InterruptedException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterSamplesize();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat(CdapUtils.pluginProp(format));
    CdfGcsActions.enterOverride(CdapUtils.pluginProp("gcsOverride"));
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfGCSLocators.getSchemaLoadComplete, 30);
  }
}
