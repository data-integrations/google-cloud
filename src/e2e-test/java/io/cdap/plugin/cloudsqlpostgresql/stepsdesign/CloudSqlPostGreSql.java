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
package io.cdap.plugin.cloudsqlpostgresql.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.cloudsqlpostgresql.actions.CdfCloudSqlPostGreSqlActions;
import io.cdap.plugin.cloudsqlpostgresql.locators.CdfCloudSqlPostGreSqlLocators;
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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_VALIDATION;

/**
 * CloudSqlPostGreSql.
 */
public class CloudSqlPostGreSql implements CdfHelper {
    List<String> propertiesOutputSchema = new ArrayList<String>();
    GcpClient gcpClient = new GcpClient();

    static PrintWriter out;
    static String rawLog;
    static int countRecords;

    @Given("Open DataFusion Project to configure pipeline")
    public void openDataFusionProjectToConfigurePipeline() throws IOException, InterruptedException {
        openCdf();
    }

    @When("Source is CloudSQLPostGreSQL")
    public void sourceIsCloudSQLPostGreSQL() throws InterruptedException {
        CdfCloudSqlPostGreSqlActions.selectCloudSQLPostGreSQLSource();
    }

    @When("Target is CloudSQLPostGreSQL")
    public void targetIsCloudSQLPostGreSQL() throws InterruptedException {
        CdfCloudSqlPostGreSqlActions.selectCloudSQLPostGreSQLSink();
    }

    @When("Target is BigQuery")
    public void targetIsBigQuery() {
        CdfStudioActions.sinkBigQuery();
    }

    @Then("Open CloudSQLPostGreSQL Properties")//need to remove
    public void openCloudSQLPostGreSQLProperties() throws InterruptedException {
        CdfCloudSqlPostGreSqlActions.clickCloudSqlPostGreSqlProperties();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlPostGreSqlLocators.validateBtn, 10);
    }

    @Then("Validate Connector properties")
    public void validatePipeline() throws InterruptedException {
        CdfCloudSqlPostGreSqlActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlPostGreSqlLocators.closeButton, 10);
    }

    @Then("Enter Reference Name & Connection Name with Invalid Test Data in Sink")
    public void enterTheSinkInvalidData() throws InterruptedException, IOException {
        CdfCloudSqlPostGreSqlActions.clickCloudSqlPostGreSqlProperties();
        CdfCloudSqlPostGreSqlActions.enterReferenceName(CdapUtils.pluginProp("pSqlReferenceNameInvalid"));
        CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp("pSqlDatabaseName"));
        CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("pSqlConnectionNameInvalid"));
        CdfCloudSqlPostGreSqlActions.enterTableName(CdapUtils.pluginProp("pSqlTableNameCS"));
    }

    @Then("Verify Reference Name Connection Name Fields with Invalid Test Data")
    public void verifyTheCldMySqlInvalidTestData() throws InterruptedException {
        Assert.assertTrue(CdfCloudSqlPostGreSqlLocators.referenceNameError.isDisplayed());
        Assert.assertTrue(CdfCloudSqlPostGreSqlLocators.connectionNameFormatError.isDisplayed());
    }

    @Then("Enter Connection Name with private instance type")
    public void enterTheInvalidPrivate() throws InterruptedException, IOException {
        CdfCloudSqlPostGreSqlActions.clickPrivateInstance();
        CdfCloudSqlPostGreSqlActions.clickValidateButton();
    }

    @Then("Verify Connection Name with private instance type")
    public void verifyTheCldMySqlInvalidPrivate() throws InterruptedException {
        Assert.assertTrue(CdfCloudSqlPostGreSqlLocators.connectionNameError.isDisplayed());
    }

    @Then("Enter the CloudSQLPostGreSQL Source Properties with blank property {string}")
    public void enterTheCloudSQLPostGreSQLSourcePropertiesWithBlankProperty(String property) throws IOException,
      InterruptedException {
        if (property.equalsIgnoreCase("referenceName")) {
            CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp("pSqlDatabaseName"));
            CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("pSqlConnectionNameValid"));
            CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp("pSqlImportQuery"));
        } else if (property.equalsIgnoreCase("database")) {
            CdfCloudSqlPostGreSqlActions.enterReferenceName(CdapUtils.pluginProp("pSqlReferenceNameValid"));
            CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("pSqlConnectionNameValid"));
            CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp("pSqlImportQuery"));
        } else if (property.equalsIgnoreCase("connectionName")) {
            CdfCloudSqlPostGreSqlActions.enterReferenceName(CdapUtils.pluginProp("pSqlReferenceNameValid"));
            CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp("pSqlDatabaseName"));
            CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp("pSqlImportQuery"));
        } else if (property.equalsIgnoreCase("importQuery")) {
            CdfCloudSqlPostGreSqlActions.enterReferenceName(CdapUtils.pluginProp("pSqlReferenceNameValid"));
            CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp("pSqlDatabaseName"));
            CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("pSqlConnectionNameValid"));
        } else if (property.equalsIgnoreCase("jdbcPluginName")) {
            CdfCloudSqlPostGreSqlActions.enterReferenceName(CdapUtils.pluginProp("pSqlReferenceNameValid"));
            SeleniumHelper.replaceElementValue(CdfCloudSqlPostGreSqlLocators.driverName, "");
            CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp("pSqlDatabaseName"));
            CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("pSqlConnectionNameValid"));
            CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp("pSqlImportQuery"));
        }
    }

    @Then("Enter the CloudSQLPostGreSQL Sink Properties with blank property {string}")
    public void enterTheCloudSQLPostGreSQLSinkPropertiesWithBlankProperty(String property) throws IOException,
      InterruptedException {
        if (property.equalsIgnoreCase("referenceName")) {
            CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp("pSqlDatabaseName"));
            CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("pSqlConnectionNameValid"));
            CdfCloudSqlPostGreSqlActions.enterTableName(CdapUtils.pluginProp("pSqlTableNameCS"));
        } else if (property.equalsIgnoreCase("database")) {
            CdfCloudSqlPostGreSqlActions.enterReferenceName(CdapUtils.pluginProp("pSqlReferenceNameValid"));
            CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("pSqlConnectionNameValid"));
            CdfCloudSqlPostGreSqlActions.enterTableName(CdapUtils.pluginProp("pSqlTableNameCS"));
        } else if (property.equalsIgnoreCase("connectionName")) {
            CdfCloudSqlPostGreSqlActions.enterReferenceName(CdapUtils.pluginProp("pSqlReferenceNameValid"));
            CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp("pSqlDatabaseName"));
            CdfCloudSqlPostGreSqlActions.enterTableName(CdapUtils.pluginProp("pSqlTableNameCS"));
        } else if (property.equalsIgnoreCase("tableName")) {
            CdfCloudSqlPostGreSqlActions.enterReferenceName(CdapUtils.pluginProp("pSqlReferenceNameValid"));
            CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp("pSqlDatabaseName"));
            CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("pSqlConnectionNameValid"));
        } else if (property.equalsIgnoreCase("jdbcPluginName")) {
            CdfCloudSqlPostGreSqlActions.enterReferenceName(CdapUtils.pluginProp("pSqlReferenceNameValid"));
            SeleniumHelper.replaceElementValue(CdfCloudSqlPostGreSqlLocators.driverName, "");
            CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp("pSqlDatabaseName"));
            CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("pSqlConnectionNameValid"));
            CdfCloudSqlPostGreSqlActions.enterTableName(CdapUtils.pluginProp("pSqlTableNameCS"));
        }
    }

    @Then("Validate mandatory property error for {string}")
    public void validateMandatoryPropertyErrorFor(String property) {
        CdfStudioActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton, 5L);
        CdapUtils.validateMandatoryPropertyError(property);
    }

    @Then("Enter Reference Name & Database Name with Test Data")
    public void enterTheValidTestData() throws InterruptedException, IOException {
        CdfCloudSqlPostGreSqlActions.enterReferenceName(CdapUtils.pluginProp("pSqlReferenceNameValid"));
        CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp("pSqlDatabaseName"));
    }

    @Then("Enter Table Name {string} and Connection Name {string}")
    public void enterTableNameInTableField(String tableName, String connectionName) throws IOException {
        CdfCloudSqlPostGreSqlActions.enterTableName(CdapUtils.pluginProp(tableName));
        CdfCloudSqlPostGreSqlActions.clickPrivateInstance();
        CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp(connectionName));
    }

    @Then("Enter Driver Name with Invalid value")
    public void enterDriverNameDefaultValue() throws IOException {
        CdfCloudSqlPostGreSqlActions.enterDriverName(CdapUtils.pluginProp("pSqlDriverNameInvalid"));
        CdfCloudSqlPostGreSqlActions.clickValidateButton();
    }

    @Then("Verify Driver Name field with Invalid value entered")
    public void verifyDriverNameFieldWithInvalidValueEntered() {
        Assert.assertTrue(CdfCloudSqlPostGreSqlLocators.driverNameError.isDisplayed());
    }

    @Then("Close the CloudSQLPostGreSQL Properties")
    public void closeTheCloudSQLPostGreSQLProperties() {
        CdfCloudSqlPostGreSqlActions.closeButton();
    }

    @Then("Enter Connection Name and Import Query {string}")
    public void enterConnectionImportField(String query) throws IOException, InterruptedException {
        CdfCloudSqlPostGreSqlActions.clickPrivateInstance();
        CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("pSqlConnectionNameValid"));
        CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp(query));
    }

    @Then("Enter Reference Name & Connection Name with Invalid Test Data and import query {string}")
    public void enterReferenceNameConnectionNameWithInvalidTestDataAndImportQuery(String query) throws
      InterruptedException, IOException {
        CdfCloudSqlPostGreSqlActions.clickCloudSqlPostGreSqlProperties();
        CdfCloudSqlPostGreSqlActions.enterReferenceName(CdapUtils.pluginProp("pSqlReferenceNameInvalid"));
        CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp("pSqlDatabaseName"));
        CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("pSqlConnectionNameInvalid"));
        CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp(query));
        SeleniumHelper.waitAndClick(CdfCloudSqlPostGreSqlLocators.validateBtn, 50);
    }

    @Then("Open cloudSQLPostgreSQL Properties")
    public void openCloudSQLPostgreSQLProperties() {
        CdfCloudSqlPostGreSqlActions.clickCloudSqlPostGreSqlProperties();
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryToGetAllValues
      (String database, String importquery) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp(importquery));
    }

    public void enterTheCloudSQLPostgreSQLPropertiesForDatabase(String database) throws IOException {
        CdfCloudSqlPostGreSqlActions.enterReferenceName("cloudSQLPostgreSQL" + UUID.randomUUID().toString());
        CdfCloudSqlPostGreSqlActions.enterDatabaseName(CdapUtils.pluginProp(database));
        CdfCloudSqlPostGreSqlActions.enterUserName(CdapUtils.pluginProp("cloudPostgresSQLUserName"));
        CdfCloudSqlPostGreSqlActions.enterPassword(CdapUtils.pluginProp("cloudPostgresSQLPassword"));
        CdfCloudSqlPostGreSqlActions.clickPrivateInstance();
        CdfCloudSqlPostGreSqlActions.enterConnectionName(CdapUtils.pluginProp("cloudPostgresSQLConnectionName"));
    }

    @Then("Capture output schema")
    public void captureOutputSchema() {
        CdfCloudSqlPostGreSqlActions.getSchema();
        WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 10);
        wait.until(ExpectedConditions.numberOfElementsToBeMoreThan
          (By.xpath("//*[@placeholder=\"Field name\"]"), 1));
        SeleniumHelper.waitElementIsVisible(SeleniumDriver.getDriver().findElement(
          By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']")), 10L);
        List<WebElement> propertiesOutputSchemaElements = SeleniumDriver.getDriver().findElements(
          By.xpath("//div[@data-cy='schema-fields-list']//*[@placeholder='Field name']"));
        for (WebElement element : propertiesOutputSchemaElements) {
            propertiesOutputSchema.add(element.getAttribute("value"));
        }
        Assert.assertTrue(propertiesOutputSchema.size() >= 2);
    }

    @Then("Validate cloudSQLPostgreSQL properties")
    public void validateCloudSQLPostgreSQLProperties() {
        CdfCloudSqlPostGreSqlActions.clickValidateButton();
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlPostGreSqlLocators.validateBtn);
        String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_VALIDATION);
        String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Close the cloudSQLPostgreSQL properties")
    public void closeTheCloudSQLPostgreSQLProperties() {
        CdfCloudSqlPostGreSqlActions.closeButton();
    }

    @Then("Open BigQuery Target Properties")
    public void openBigQueryTargetProperties() {
        CdfStudioActions.clickProperties("BigQuery");
    }

    @Then("Enter the BigQuery Target Properties for table {string}")
    public void enterTheBigQueryTargetPropertiesForTable(String tableName) throws IOException {
        CdfBigQueryPropertiesActions.enterProjectId(CdapUtils.pluginProp("projectId"));
        CdfBigQueryPropertiesActions.enterDatasetProjectId(CdapUtils.pluginProp("projectId"));
        CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_Ref_" + UUID.randomUUID().toString());
        CdfBigQueryPropertiesActions.enterBigQueryDataset(CdapUtils.pluginProp("dataset"));
        CdfBigQueryPropertiesActions.enterBigQueryTable(CdapUtils.pluginProp(tableName));
    }

    @Then("Validate Bigquery properties")
    public void validateBigqueryProperties() {
        CdfGcsActions.clickValidateButton();
        String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_VALIDATION);
        String actualErrorMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
        Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    }

    @Then("Close the BigQuery properties")
    public void closeTheBigQueryProperties() {
        CdfStudioActions.clickCloseButton();
    }

    @Then("Connect Source as {string} and sink as {string} to establish connection")
    public void connectSourceAsAndSinkAsToEstablishConnection(String source, String sink) {
        CdfStudioActions.connectSourceAndSink(source, sink);
    }

    @Then("Add pipeline name")
    public void addPipelineName() {
        CdfStudioActions.pipelineName();
        CdfStudioActions.pipelineNameIp("cloudSQLPostgreSQL_BQ" + UUID.randomUUID().toString());
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

    @Then("Click on PreviewData for cloudSQLPostgreSQL")
    public void clickOnPreviewDataForCloudSQLPostgreSQL() {
        CdfCloudSqlPostGreSqlActions.clickPreviewData();
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
    public void closeThePreviewAndDeployThePipeline() {
        SeleniumHelper.waitAndClick(CdfStudioLocators.closeButton, 5L);
        CdfStudioActions.previewSelect();
        SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pipelineDeploy, 2);
        CdfStudioActions.pipelineDeploy();
    }

    @Then("Open the Logs and capture raw logs")
    public void openTheLogsAndCaptureRawLogs() {
        CdfPipelineRunAction.logsClick();
        rawLog = CdfPipelineRunAction.captureRawLogs();
        SeleniumDriver.getDriver().navigate().refresh();
        BeforeActions.scenario.write(rawLog);
        out.println(rawLog);
        out.close();
    }

    @Then("Validate records out from cloudSQLPostgreSQL is equal to records transferred in " +
      "BigQuery {string} output records")
    public void validateRecordsOutFromCloudSQLPostgreSQLIsEqualToRecordsTransferredInBigQueryOutputRecords
      (String tableName) throws IOException, InterruptedException {
        int countRecords;
        countRecords = gcpClient.countBqQuery(CdapUtils.pluginProp(tableName));
        Assert.assertEquals(countRecords, recordOut());
    }

    @Then("Run the Pipeline in Runtime")
    public void runThePipelineInRuntime() throws InterruptedException {
        CdfPipelineRunAction.runClick();
    }

    @Then("Wait till pipeline is in running state")
    public void waitTillPipelineIsInRunningState() throws InterruptedException {
        Boolean bool = true;
        WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 1000000);
        wait.until(ExpectedConditions.or
          (ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
           ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
    }

    @Then("Verify the pipeline status is {string}")
    public void verifyThePipelineStatusIs(String status) {
        boolean webelement = false;
        webelement = SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']");
        Assert.assertTrue(webelement);
    }

    @Then("Get Count of no of records transferred to BigQuery in {string}")
    public void getCountOfNoOfRecordsTransferredToBigQueryIn(String arg0) throws IOException, InterruptedException {
        int countRecords;
        countRecords = gcpClient.countBqQuery(SeleniumHelper.readParameters(arg0));
        BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
        Assert.assertTrue(countRecords > 0);
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string} for {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForNull
      (String database, String importquery, String splitColumnValue) throws IOException, InterruptedException {
      enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp(importquery));
        CdfCloudSqlPostGreSqlActions.enterSplitColumn(CdapUtils.pluginProp(splitColumnValue));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using " +
      "query {string} for between values {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForBetweenValues
      (String database, String importquery, String cloudPostgresSQLSplitColumnBtwnValue)
      throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp(importquery));
        CdfCloudSqlPostGreSqlActions.enterSplitColumn(CdapUtils.pluginProp(cloudPostgresSQLSplitColumnBtwnValue));
    }


    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string} for max and min {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForMaxAndMin
      (String database, String importquery, String splitColumnField) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
       CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp(importquery));
        CdfCloudSqlPostGreSqlActions.enterSplitColumn(CdapUtils.pluginProp(splitColumnField));

    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string} " +
      "for duplicate values {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForDuplicateValues
      (String database, String importquery, String splitColumnField) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp(importquery));
        CdfCloudSqlPostGreSqlActions.enterSplitColumn(CdapUtils.pluginProp(splitColumnField));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string} for max values {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForMaxValues
      (String database, String importquery, String splitColumnField) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp(importquery));
        CdfCloudSqlPostGreSqlActions.enterSplitColumn(CdapUtils.pluginProp(splitColumnField));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using query {string} for min values {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForMinValues
      (String database, String importQuery, String splitCoulumnField) throws IOException, InterruptedException {
      enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
     CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp(importQuery));
     CdfCloudSqlPostGreSqlActions.enterSplitColumn(CdapUtils.pluginProp(splitCoulumnField));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} " +
      "using query {string} for distinct values {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingQueryForDistinctValues
      (String database, String importQuery, String splitCoulumnField) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp(importQuery));
        CdfCloudSqlPostGreSqlActions.enterSplitColumn(CdapUtils.pluginProp(splitCoulumnField));
    }

    @Then("Enter the cloudSQLPostgreSQL properties for database {string} using different join queries {string}")
    public void enterTheCloudSQLPostgreSQLPropertiesForDatabaseUsingDifferentJoinQueries
      (String database, String importQuery) throws IOException, InterruptedException {
        enterTheCloudSQLPostgreSQLPropertiesForDatabase(database);
        CdfCloudSqlPostGreSqlActions.enterImportQuery(CdapUtils.pluginProp(importQuery));
    }
}

