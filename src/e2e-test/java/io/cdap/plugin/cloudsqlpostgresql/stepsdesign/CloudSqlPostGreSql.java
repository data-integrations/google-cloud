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

import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.GcpClient;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.cloudsqlpostgresql.actions.CdfCloudSqlPostGreSqlActions;
import io.cdap.plugin.cloudsqlpostgresql.locators.CdfCloudSqlPostGreSqlLocators;
import io.cdap.plugin.utils.CdapUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * CloudSqlPostGreSql.
 */
public class CloudSqlPostGreSql implements CdfHelper {
    List<String> propertiesOutputSchema = new ArrayList<String>();
    static int i = 0;
    GcpClient gcpClient = new GcpClient();

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

    @Then("Open CloudSQLPostGreSQL Properties")
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
}
