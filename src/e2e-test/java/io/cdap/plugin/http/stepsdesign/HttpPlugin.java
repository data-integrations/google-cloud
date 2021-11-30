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
package io.cdap.plugin.http.stepsdesign;

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
import io.cdap.plugin.http.actions.HttpPluginActions;
import io.cdap.plugin.http.locators.HttpPluginLocators;
import io.cdap.plugin.utils.CdapUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import stepsdesign.BeforeActions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

/**
 * DemoTestCase GCP.
 */
public class HttpPlugin implements CdfHelper {
    GcpClient gcpClient = new GcpClient();

    @Given("Open Datafusion Project to configure pipeline")
    public void openDatafusionProjectToConfigurePipeline() throws IOException, InterruptedException {
        openCdf();
    }

    @When("Source is HTTP")
    public void sourceIsHTTP() {
        HttpPluginActions.clickHttpSource();
    }

    @When("Target is HTTP")
    public void targetIsHTTP() {
        HttpPluginActions.clickHttpSink();
    }

    @Then("Verify mandatory fields")
    public void verifyMandatoryFields() {
        HttpPluginActions.clickValidateButton();
        HttpPluginActions.validateReferenceNameError();
        HttpPluginActions.validateHTTPURLError();;
    }

    @Then("Verify validation error")
    public void verifyValidationError() {
        HttpPluginActions.clickValidateButton();
        HttpPluginActions.validateValidationError();
    }

    @When("Target is BigQuery")
    public void targetIsBigQuery() {
        CdfStudioActions.sinkBigQuery();
    }

    @Then("Open HTTP Properties")
    public void openHTTPProperties() {
        HttpPluginActions.clickHttpProperties();
    }

    @Then("Link Source HTTP and Sink Bigquery to establish connection")
    public void linkSourceAndSinkToEstablishConnection() throws InterruptedException {
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.toBigQiery);
        SeleniumHelper.dragAndDrop(HttpPluginLocators.fromHttp, CdfStudioLocators.toBigQiery);
    }

    @Then("Enter the HTTP Properties with {string} and {string}")
    public void enterTheHTTPProperties(String outputSchema, String jsonFieldsMapping) throws InterruptedException {
        HttpPluginActions.enterReferenceName(CdapUtils.pluginProp("httpSrcReferenceName"));
        HttpPluginActions.enterHttpUrl(CdapUtils.pluginProp("httpSrcUrl"));
        HttpPluginActions.selectHttpMethodSource(CdapUtils.pluginProp("httpSrcMethod"));
        HttpPluginActions.enterJsonXmlResultPath(CdapUtils.pluginProp("httpSrcResultPath"));
        HttpPluginActions.enterJSONXmlFieldsMapping(CdapUtils.pluginProp(jsonFieldsMapping));
        HttpPluginActions.selectFormat(CdapUtils.pluginProp("httpSrcFormat"));
        HttpPluginActions.enterOutputSchema(CdapUtils.pluginProp(outputSchema));
        HttpPluginActions.clickValidateButton();
    }

    @Then("Close the HTTP Properties")
    public void closeTheHTTPProperties() {
        HttpPluginActions.clickCloseButton();
    }

    @Then("Enter the BigQuery Properties for table {string}")
    public void enterTheBigQueryPropertiesForTable(String tableName) throws InterruptedException, IOException {
        CdfBigQueryPropertiesActions.enterBigQueryProperties(tableName);
    }

    @Then("Close the BigQuery Properties")
    public void closeTheBigQueryProperties() {
        CdfGcsActions.closeButton();
    }

    @Then("Save and Deploy Pipeline")
    public void saveAndDeployPipeline() throws InterruptedException {
        CdfStudioActions.pipelineName();
        CdfStudioActions.pipelineNameIp("TestPipeline" + UUID.randomUUID().toString());
        CdfStudioActions.pipelineSave();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineDeploy, 2);
        CdfStudioActions.pipelineDeploy();
    }

    @Then("Run the Pipeline in Runtime")
    public void runThePipelineInRuntime() throws InterruptedException {
        CdfPipelineRunAction.runClick();
    }

    @Then("Wait till pipeline is in running state")
    public void waitTillPipelineIsInRunningState() throws InterruptedException {
        Boolean bool = true;
        WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 180);
        wait.until(ExpectedConditions.or(
          ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
          ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
    }

    @Then("Open Logs")
    public void openLogs() throws FileNotFoundException, InterruptedException {
        CdfPipelineRunAction.logsClick();
        BeforeActions.scenario.write(CdfPipelineRunAction.captureRawLogs());
        PrintWriter out = new PrintWriter(BeforeActions.myObj);
        out.println(CdfPipelineRunAction.captureRawLogs());
        out.close();
    }

    @Then("Verify the pipeline status is {string}")
    public void verifyThePipelineStatusIs(String status) {
        boolean webelement = false;
        webelement = SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']");
        Assert.assertTrue(webelement);
    }

    @Then("validate successMessage is displayed")
    public void validateSuccessMessageIsDisplayed() {
        CdfLogActions.validateSucceeded();
    }

    @Then("Get Count of no of records transferred to BigQuery in {string}")
    public void getCountOfNoOfRecordsTransferredToBigQueryIn(String arg0) throws IOException, InterruptedException {
        int countRecords;
        countRecords = gcpClient.countBqQuery(SeleniumHelper.readParameters(arg0));
        BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
        Assert.assertTrue(countRecords > 0);
    }

    @When("Source is GCS bucket")
    public void sourceIsGCSBucket() throws InterruptedException {
        CdfStudioActions.selectGCS();
    }

    @Then("Enter the GCS Properties with {string} GCS bucket")
    public void enterTheGCSPropertiesWithGCSBucket(String bucket) throws InterruptedException, IOException {
        CdfGcsActions.gcsProperties();
        CdfGcsActions.enterReferenceName();
        CdfGcsActions.enterProjectId();
        CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
        CdfGcsActions.selectFormat("csv");
        CdfGcsActions.skipHeader();
        CdfGcsActions.getSchema();
        SeleniumHelper.waitElementIsVisible(
          SeleniumDriver.getDriver().findElement(
            By.xpath("//*[@data-cy='get-schema-btn']//span[text()='Get Schema']")));
    }

    @Then("Close the GCS Properties")
    public void closeTheGCSProperties() {
        CdfGcsActions.closeButton();
    }

    @Then("Link Source GCS and Sink HTTP to establish connection")
    public void linkSourceGCSAndSinkHTTPToEstablishConnection() {
        SeleniumHelper.waitElementIsVisible(HttpPluginLocators.toHTTP);
        SeleniumHelper.dragAndDrop(CdfStudioLocators.fromGCS, HttpPluginLocators.toHTTP);
    }

    @Then("Enter the HTTP Sink Properties")
    public void enterTheHTTPSinkProperties() {
        HttpPluginActions.enterReferenceName(CdapUtils.pluginProp("httpSinkReferenceName"));
        HttpPluginActions.enterHttpUrl(CdapUtils.pluginProp("httpSinkUrl"));
        HttpPluginActions.selectHttpMethodSink(CdapUtils.pluginProp("httpSinkMethod"));
        HttpPluginActions.selectSinkMessageFormat(CdapUtils.pluginProp("httpSinkMessageFormat"));
        HttpPluginActions.clickValidateButton();
    }

}


