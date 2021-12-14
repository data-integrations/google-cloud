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
package io.cdap.plugin.spanner.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.spanner.actions.CdfSpannerActions;
import io.cdap.plugin.spanner.locators.CdfSpannerLocators;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_ERROR_FOUND_VALIDATION;
import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_VALIDATION;

/**
 * Step Design for Spanner plugin.
 */
public class SpannerConnector implements CdfHelper {

    List<String> propertiesOutputSchema = new ArrayList<String>();
    GcpClient gcpClient = new GcpClient();

    @Given("Open Datafusion Project to configure pipeline")
    public void openDatafusionProjectToConfigurePipeline() throws IOException, InterruptedException {
        openCdf();
    }

    @When("Source is Spanner Connector")
    public void sourceIsSpannerConnector() throws InterruptedException {
        CdfSpannerActions.selectSpanner();
    }

    @Then("Open Spanner connector properties")
    public void openSpannerConnectorProperties() {
        CdfStudioActions.clickProperties("Spanner");
    }

    @Then("Enter the Spanner connector Properties")
    public void enterTheSpannerConnectorProperties() throws IOException {
        CdfSpannerActions.enterProjectId(CdapUtils.pluginProp("projectId"));
        CdfSpannerActions.enterReferenceName();
        CdfSpannerActions.enterInstanceID(CdapUtils.pluginProp("spannerInstanceId"));
        CdfSpannerActions.enterDatabaseename(CdapUtils.pluginProp("spannerDatabaseName"));
        CdfSpannerActions.enterTablename(CdapUtils.pluginProp("spannerTablename"));
    }

    @When("Target is BigQuery")
    public void targetIsBigQuery() {
        CdfStudioActions.sinkBigQuery();
    }

    @Then("Open BigQuery Properties")
    public void openBigQueryProperties() {
        CdfStudioActions.clickProperties("BigQuery");
    }

    @Then("Enter the BigQuery Sink properties for table {string}")
    public void enterTheBigQuerySinkPropertiesForTable(String tableName) throws IOException {
        CdfBigQueryPropertiesActions.enterProjectId(CdapUtils.pluginProp("projectId"));
        CdfBigQueryPropertiesActions.enterDatasetProjectId(CdapUtils.pluginProp("projectId"));
        CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_File_Ref_" + UUID.randomUUID().toString());
        CdfBigQueryPropertiesActions.enterBigQueryDataset(CdapUtils.pluginProp("dataset"));
        CdfBigQueryPropertiesActions.enterBigQueryTable(CdapUtils.pluginProp(tableName));
        CdfBigQueryPropertiesActions.clickUpdateTable();
        CdfBigQueryPropertiesActions.clickTruncatableSwitch();
    }

    @Then("Get Count of no of records transferred to BigQuery in {string}")
    public void getCountOfNoOfRecordsTransferredToBigQueryIn(String tableName)
      throws IOException, InterruptedException {
        int countRecords = gcpClient.countBqQuery(CdapUtils.pluginProp(tableName));
        BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
        Assert.assertTrue(countRecords > 0);
    }

    @Then("Validate BigQuery properties")
    public void validateBigQueryProperties() {
        CdfSpannerActions.clickValidateButton();
        String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_VALIDATION);
        String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Close the BigQuery Properties")
    public void closeTheBigQueryProperties() {
        CdfSpannerActions.closeButton();
    }

    @Then("Capture and validate output schema")
    public void captureAndValidateOutputSchema() {
        CdfSpannerActions.getSchema();
        SeleniumHelper.waitElementIsVisible(CdfSpannerLocators.getSchemaLoadComplete, 10L);
        Assert.assertFalse(SeleniumHelper.isElementPresent(CdfStudioLocators.pluginValidationErrorMsg));
        By schemaXpath = By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']");
        SeleniumHelper.waitElementIsVisible(SeleniumDriver.getDriver().findElement(schemaXpath), 2L);
        List<WebElement> propertiesOutputSchemaElements = SeleniumDriver.getDriver().findElements(schemaXpath);
        for (WebElement element : propertiesOutputSchemaElements) {
            propertiesOutputSchema.add(element.getAttribute("value"));
        }
        Assert.assertTrue(propertiesOutputSchema.size() >= 1);
    }

    @Then("Validate Spanner connector properties")
    public void thenValidateSpannerConnectorProperties() {
        CdfSpannerActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pluginValidationSuccessMsg, 10L);
        String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_VALIDATION);
        String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Close the Spanner Properties")
    public void closeTheSpannerProperties() {
        CdfSpannerActions.closeButton();
    }

    @Then("Enter the GCS Properties and {string} file format")
    public void enterTheGCSPropertiesAndFileFormat(String fileFormat) throws IOException, InterruptedException {
        CdfGcsActions.gcsProperties();
        CdfGcsActions.enterReferenceName();
        CdfGcsActions.enterProjectId();
        CdfGcsActions.getGcsBucket(CdapUtils.pluginProp("spannerPathGCS"));
        CdfGcsActions.selectFormat(fileFormat);
        CdfGcsActions.clickValidateButton();
    }

    @Then("Validate GCS properties")
    public void validateGCSProperties() {
        CdfSpannerActions.clickValidateButton();
        String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_VALIDATION);
        String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @When("Target is GCS")
    public void targetIsGCS() throws InterruptedException {
        CdfStudioActions.sinkGcs();
    }

    @Then("Connect Source as {string} and sink as {string} to establish connection")
    public void connectSourceAsAndSinkAsToEstablishConnection(String source, String sink) {
        CdfStudioActions.connectSourceAndSink(source, sink);
    }

    @Then("Then Validate GCS properties")
    public void thenValidateGCSProperties() {
        CdfSpannerActions.clickValidateButton();
        String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_VALIDATION);
        String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Close the GCS Properties")
    public void closeTheGCSProperties() {
        CdfGcsActions.closeButton();
    }

    @Then("Save the pipeline")
    public void saveThePipeline() {
        CdfStudioActions.pipelineName();
        CdfStudioActions.pipelineNameIp("Spanner_BQ_" + UUID.randomUUID().toString());
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

    @Then("Click on PreviewData for Spanner connector")
    public void clickOnPreviewDataForSpannerConnector() {
        CdfSpannerActions.clickPreviewData();
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

    @Then("Close the Preview")
    public void closeThePreview() {
        CdfSpannerLocators.closeButton.click();
        CdfStudioActions.previewSelect();
    }

    @Then("Deploy the pipeline")
    public void deployThePipeline() {
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineDeploy, 2);
        CdfStudioActions.pipelineDeploy();
    }

    @Then("Run the Pipeline in Runtime")
    public void runThePipelineInRuntime() throws InterruptedException {
        CdfPipelineRunAction.runClick();
    }

    @Then("Wait till pipeline is in running state")
    public void waitTillPipelineIsInRunningState() {
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

    @Then("Validate successMessage is displayed")
    public void validateSuccessMessageIsDisplayed() {
        CdfLogActions.validateSucceeded();
    }

    @Then("Open Logs")
    public void openLogs() throws FileNotFoundException, InterruptedException {
        CdfPipelineRunAction.logsClick();
    }
    @Then("Validate mandatory property error for {string}")
    public void validateMandatoryPropertyErrorFor(String property) {
        CdfStudioActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton);
        CdapUtils.validateMandatoryPropertyError(property);
    }
    @Then("Enter the Spanner connector Properties with blank property {string}")
    public void enterTheSpannerConnectorPropertiesWithBlankProperty(String property) throws IOException {

        if (property.equalsIgnoreCase("referenceName")) {
            CdfSpannerActions.enterInstanceID(CdapUtils.pluginProp("spannerInstanceId"));
            CdfSpannerActions.enterDatabaseename(CdapUtils.pluginProp("spannerDatabaseName"));
            CdfSpannerActions.enterTablename(CdapUtils.pluginProp("spannerTablename"));
        } else if (property.equalsIgnoreCase("instance")) {
            CdfSpannerActions.enterReferenceName();
            CdfSpannerActions.enterDatabaseename(CdapUtils.pluginProp("spannerDatabaseName"));
            CdfSpannerActions.enterTablename(CdapUtils.pluginProp("spannerTablename"));
        } else if (property.equalsIgnoreCase("database")) {
            CdfSpannerActions.enterReferenceName();
            CdfSpannerActions.enterInstanceID(CdapUtils.pluginProp("spannerInstanceId"));
            CdfSpannerActions.enterTablename(CdapUtils.pluginProp("spannerTablename"));
        } else if (property.equalsIgnoreCase("table")) {
            CdfSpannerActions.enterReferenceName();
            CdfSpannerActions.enterInstanceID(CdapUtils.pluginProp("spannerInstanceId"));
            CdfSpannerActions.enterDatabaseename(CdapUtils.pluginProp("spannerDatabaseName"));
        }
    }

    @Then("Enter the Spanner connector Properties with incorrect property {string}")
    public void enterTheSpannerConnectorPropertiesWithIncorrectProperty(String property) throws IOException {
        CdfSpannerActions.enterProjectId(CdapUtils.pluginProp("projectId"));
        CdfSpannerActions.enterReferenceName();
        if (property.equalsIgnoreCase("instance")) {
            CdfSpannerActions.enterInstanceID(CdapUtils.pluginProp("spannerIncorrectInstanceId"));
            CdfSpannerActions.enterDatabaseename(CdapUtils.pluginProp("spannerDatabaseName"));
            CdfSpannerActions.enterTablename(CdapUtils.pluginProp("spannerTablename"));
        } else if (property.equalsIgnoreCase("database")) {
            CdfSpannerActions.enterInstanceID(CdapUtils.pluginProp("spannerInstanceId"));
            CdfSpannerActions.enterDatabaseename(CdapUtils.pluginProp("spannerIncorrectDatabaseName"));
            CdfSpannerActions.enterTablename(CdapUtils.pluginProp("spannerTablename"));
        } else if (property.equalsIgnoreCase("table")) {
            CdfSpannerActions.enterInstanceID(CdapUtils.pluginProp("spannerInstanceId"));
            CdfSpannerActions.enterDatabaseename(CdapUtils.pluginProp("spannerDatabaseName"));
            CdfSpannerActions.enterTablename(CdapUtils.pluginProp("spannerIncorrectTablename"));
        } else if (property.equalsIgnoreCase("importQuery")) {
            CdfSpannerActions.enterInstanceID(CdapUtils.pluginProp("spannerInstanceId"));
            CdfSpannerActions.enterDatabaseename(CdapUtils.pluginProp("spannerDatabaseName"));
            CdfSpannerActions.enterTablename(CdapUtils.pluginProp("spannerTablename"));
            CdfSpannerActions.enterImportQuery(CdapUtils.pluginProp("spannerIncorrectQuery"));
        }
    }

    @Then("Verify plugin validation fails with error")
    public void verifyPluginValidationFailsWithError() {
        CdfStudioActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pluginValidationErrorMsg, 10L);
        String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_ERROR_FOUND_VALIDATION);
        String actualErrorMessage = CdfStudioLocators.pluginValidationErrorMsg.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Enter the Spanner connector Properties with Import Query {string}")
    public void enterTheSpannerConnectorPropertiesWithImportQuery(String query) throws IOException {
        CdfSpannerActions.enterProjectId(CdapUtils.pluginProp("projectId"));
        CdfSpannerActions.enterReferenceName();
        CdfSpannerActions.enterInstanceID(CdapUtils.pluginProp("spannerInstanceId"));
        CdfSpannerActions.enterDatabaseename(CdapUtils.pluginProp("spannerDatabaseName"));
        CdfSpannerActions.enterTablename(CdapUtils.pluginProp("spannerTablename"));
        CdfSpannerActions.enterImportQuery(CdapUtils.pluginProp(query));
    }
}
