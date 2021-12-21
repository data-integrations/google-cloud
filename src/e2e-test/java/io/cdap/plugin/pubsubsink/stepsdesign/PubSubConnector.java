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

package io.cdap.plugin.pubsubsink.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfGCSLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.pubsubsink.actions.PubSubActions;
import io.cdap.plugin.pubsubsink.locators.PubSubLocators;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cdap.plugin.utils.E2ETestUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * PubSub connector related step design.
 */

public class PubSubConnector implements CdfHelper {
  List<String> propertiesSchemaColumnList = new ArrayList<>();

  @Given("Open Datafusion Project to configure pipeline")
  public void openDatafusionProjectToConfigurePipeline() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source is GCS bucket")
  public void sourceIsGCSBucket() throws InterruptedException {
    CdfStudioActions.selectGCS();
  }

  @When("Target is PubSub")
  public void targetIsPubSub() {
    PubSubActions.sinkPubSub();
  }

  @Then("Link Source as {string} and Sink as {string} to establish connection")
  public void linkSourceAsAndSinkAsToEstablishConnection(String source, String sink) {
    if (sink.equalsIgnoreCase("Pub/Sub")) {
      sink = "GooglePublisher";
    }
    PubSubActions.connectSourceAndSink(source, sink);
  }

  @Then("Enter the GCS Properties for {string} GCS bucket with format {string}")
  public void enterTheGCSPropertiesForGCSBucketWithFormat(String bucketName, String format)
    throws IOException, InterruptedException {
    CdfGcsActions.enterProjectId();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.getGcsBucket(E2ETestUtils.pluginProp(bucketName));
    CdfGcsActions.selectFormat(format);
    CdfGcsActions.enterSampleSize("1000");
    CdfGcsActions.skipHeader();
  }

  @Then("Open GCS properties")
  public void openGCSProperties() {
    CdfStudioActions.clickProperties("GCS");
  }

  @Then("Validate the GCS properties")
  public void validateTheGCSProperties() {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pluginValidationSuccessMsg, 10L);
    String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_VALIDATION);
    String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  @Then("Capture and validate output schema")
  public void captureAndValidateOutputSchema() {
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(PubSubLocators.getSchemaLoadComplete, 10L);
    Assert.assertFalse(SeleniumHelper.isElementPresent(CdfStudioLocators.pluginValidationErrorMsg));
    By schemaXpath = By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']");
    SeleniumHelper.waitElementIsVisible(SeleniumDriver.getDriver().findElement(schemaXpath), 2L);
    List<WebElement> propertiesOutputSchemaElements = SeleniumDriver.getDriver().findElements(schemaXpath);
    for (WebElement element : propertiesOutputSchemaElements) {
      propertiesSchemaColumnList.add(element.getAttribute("value"));
    }
    Assert.assertTrue(propertiesSchemaColumnList.size() >= 1);
  }

  @Then("Close the GCS Properties")
  public void closeTheGCSProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Open the PubSub Properties")
  public void openThePubSubProperties() {
    PubSubActions.clickPluginProperties("GooglePublisher");
  }

  @Then("Enter the PubSub Properties for topic {string} and format {string}")
  public void enterThePubSubPropertiesForTopicAndFormat(String topic, String format) {
    PubSubActions.enterPubSubReferenceName();
    PubSubActions.enterProjectID(E2ETestUtils.pluginProp("projectId"));
    PubSubActions.enterPubsubTopic(E2ETestUtils.pluginProp(topic));
    PubSubActions.selectFormat(format);
  }

  @Then("Enter the PubSub advanced properties")
  public void enterThePubSubAdvancedProperties() {
    PubSubActions.enterMaximumBatchCount(E2ETestUtils.pluginProp("pubSubMaximumBatchCount"));
    PubSubActions.enterMaximumBatchSize(E2ETestUtils.pluginProp("pubSubMaximumBatchSize"));
    PubSubActions.enterPublishDelayThreshold(E2ETestUtils.pluginProp("pubSubPublishDelayThreshold"));
    PubSubActions.retryTimeOut(E2ETestUtils.pluginProp("pubSubRetryTimeOut"));
    PubSubActions.errorThreshold(E2ETestUtils.pluginProp("pubSubErrorThreshold"));
  }

  @Then("Validate the PubSub properties")
  public void validateThePubSubProperties() {
    PubSubActions.validate();
    SeleniumHelper.waitElementIsVisible(PubSubLocators.pluginValidationSuccessMsg, 10L);
    String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_VALIDATION);
    String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  @Then("Close the PubSub properties")
  public void closeThePubSubProperties() {
    PubSubActions.close();
  }

  @Then("Save and Deploy Pipeline")
  public void saveAndDeployPipeline() {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("TestPipeLine" + UUID.randomUUID().toString());
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
    boolean webelement = false;
    webelement = SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']");
    Assert.assertTrue(webelement);
  }

  @Then("Open Logs")
  public void openLogs() {
    CdfPipelineRunAction.logsClick();
  }

  @Then("Enter the PubSub property with blank property {string}")
  public void enterThePubSubPropertyWithBlankProperty(String property) throws IOException {
    if (property.equalsIgnoreCase("referenceName")) {
      PubSubActions.enterPubsubTopic("dummyTopic");
    } else if (property.equalsIgnoreCase("topic")) {
      PubSubActions.enterPubSubReferenceName();
    } else {
      Assert.fail("Invalid PubSub Mandatory Field");
    }
  }

  @Then("Validate mandatory property error for {string}")
  public void validateMandatoryPropertyErrorFor(String property) {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton);
    E2ETestUtils.validateMandatoryPropertyError(property);
  }

  @Then("Enter the PubSub advanced properties with incorrect property {string}")
  public void enterThePubSubAdvancedPropertiesWithIncorrectProperty(String property) {
    if (property.equalsIgnoreCase("messageCountBatchSize")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.maximumBatchcount,
                                         E2ETestUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("requestThresholdKB")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.maximumBatchSize,
                                         E2ETestUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("publishDelayThresholdMillis")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.publishDelayThreshold,
                                         E2ETestUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("retryTimeoutSeconds")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.retryTimeout,
                                         E2ETestUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("errorThreshold")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.errorThreshold,
                                         E2ETestUtils.pluginProp("pubSubStringValue"));
    } else {
      Assert.fail("Invalid PubSub Advanced Property");
    }
  }

  @Then("Validate the error message for property {string}")
  public void validateTheErrorMessageForProperty(String property) {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_INVALID_ADVANCED_FIELDS)
      .replaceAll("PROPERTY", property);
    String actualErrorMessage = E2ETestUtils.findPropertyErrorElement(property).getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement(property));
    String expectedColor = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_COLOR);
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Enter the PubSub advanced properties with invalid number for property {string}")
  public void enterThePubSubAdvancedPropertiesWithInvalidNumberForProperty(String property) {
    if (property.equalsIgnoreCase("messageCountBatchSize")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.maximumBatchcount,
                                         E2ETestUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("requestThresholdKB")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.maximumBatchSize,
                                         E2ETestUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("publishDelayThresholdMillis")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.publishDelayThreshold,
                                         E2ETestUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("retryTimeoutSeconds")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.retryTimeout,
                                         E2ETestUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("errorThreshold")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.errorThreshold,
                                         E2ETestUtils.pluginProp("pubSubNegativeValue"));
    } else {
      Assert.fail("Invalid PubSub Advanced Property");
    }
  }

  @Then("Validate the number format error message for property {string}")
  public void validateTheNumberFormatErrorMessageForProperty(String property) {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = StringUtils.EMPTY;
    if (property.equalsIgnoreCase("messageCountBatchSize")) {
      expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_MAX_BATCH_COUNT);
    } else if (property.equalsIgnoreCase("requestThresholdKB")) {
      expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_MAX_BATCH_SIZE);
    } else if (property.equalsIgnoreCase("publishDelayThresholdMillis")) {
      expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_DELAY_THRESHOLD);
    } else if (property.equalsIgnoreCase("retryTimeoutSeconds")) {
      expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_RETRY_TIMEOUT);
    } else if (property.equalsIgnoreCase("errorThreshold")) {
      expectedErrorMessage = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_ERROR_THRESHOLD);
    } else {
      Assert.fail("Invalid PubSub Advanced Property");
    }
    String actualErrorMessage = E2ETestUtils.findPropertyErrorElement(property).getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = E2ETestUtils.getErrorColor(E2ETestUtils.findPropertyErrorElement(property));
    String expectedColor = E2ETestUtils.errorProp(E2ETestConstants.ERROR_MSG_COLOR);
    Assert.assertEquals(expectedColor, actualColor);
  }
  
  @Then("Get Schema for GCS")
  public void getSchemaForGCS() {
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfGCSLocators.getSchemaLoadComplete);
  }

  @Then("Save the pipeline")
  public void saveThePipeline() {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("PubSub_Sink_" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.statusBanner, 5);
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

  @Then("Click on PreviewData for PubSub Connector")
  public void clickOnPreviewDataForPubSubConnector() {
      PubSubActions.clickPreviewData();
    }

  @Then("Verify Preview output schema matches the outputSchema captured in properties")
  public void verifyPreviewOutputSchemaMatchesTheOutputSchemaCapturedInProperties() {
    List<String> previewSchemaColumnList = new ArrayList<>();
    List<WebElement> previewOutputSchemaElements = SeleniumDriver.getDriver().findElements(
      By.xpath("(//h2[text()='Output Records']/parent::div/div/div/div/div)[1]//div[text()!='']"));
    for (WebElement element : previewOutputSchemaElements) {
      previewSchemaColumnList.add(element.getAttribute("title"));
    }
    Assert.assertTrue(previewSchemaColumnList.equals(propertiesSchemaColumnList));
  }

  @Then("Close the Preview")
  public void closeThePreview() {
    PubSubLocators.closeButton.click();
    CdfStudioActions.previewSelect();
  }

  @Then("Deploy the pipeline")
  public void deployThePipeline() {
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineDeploy, 20);
    CdfStudioActions.pipelineDeploy();
  }

  @Then("Validate successMessage is displayed")
  public void validateSuccessMessageIsDisplayed() {
    CdfLogActions.validateSucceeded();
  }

  @Then("Click on PreviewData for GCS Connector")
  public void clickOnPreviewDataForGCSConnector() {
    CdfGcsActions.clickPreviewData();
  }

  @When("Source is BigQuery")
  public void sourceIsBigQuery() throws InterruptedException {
    CdfStudioActions.selectBQ();
  }

  @Then("Open BigQuery Properties")
  public void openBigQueryProperties() {
    CdfStudioActions.clickProperties("BigQuery");
  }

  @Then("Enter the BigQuery properties for table {string}")
  public void enterTheBigQueryPropertiesForTable(String tableName) throws IOException {
    CdfGcsActions.enterProjectId();
    CdfBigQueryPropertiesActions.enterProjectId(E2ETestUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(E2ETestUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_Ref_" + UUID.randomUUID().toString());
    CdfBigQueryPropertiesActions.enterBigQueryDataset(E2ETestUtils.pluginProp("dataset"));
    CdfBigQueryPropertiesActions.enterBigQueryTable(E2ETestUtils.pluginProp(tableName));
  }

  @Then("Close the BigQuery properties")
  public void closeTheBigQueryProperties() {
    CdfStudioActions.clickCloseButton();
  }

  @Then("Click on PreviewData for BigQuery Connector")
  public void clickOnPreviewDataForBigQueryConnector() {
    CdfBigQueryPropertiesActions.clickPreviewData();
  }
}
