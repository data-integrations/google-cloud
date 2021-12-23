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
package io.cdap.plugin.cloudsqlpostgresql.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * CloudSqlPostGreSql Connector Locators.
 */

public class CdfCloudSqlPostGreSqlLocators {
    @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-CloudSQLPostgreSQL-batchsource']")
    public static WebElement cloudSqlPsqlSource;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-CloudSQLPostgreSQL-batchsink']")
    public static WebElement cloudSqlPsqlSink;

    @FindBy(how = How.XPATH, using = "//*[text()='Sink ']")
    public static WebElement sink;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='referenceName' and @class='MuiInputBase-input']")
    public static WebElement referenceName;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'Invalid reference name')]")
    public static WebElement referenceNameError;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='jdbcPluginName' and @class='MuiInputBase-input']")
    public static WebElement driverName;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='database' and @class='MuiInputBase-input']")
    public static WebElement database;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='importQuery']//textarea")
    public static WebElement importQuery;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='boundingQuery']//textarea")
    public static WebElement boundingQuery;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'JDBC Driver class for')]")
    public static WebElement driverNameError;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='user' and @class='MuiInputBase-input']")
    public static WebElement username;

    @FindBy(how = How.XPATH, using = "//*[@placeholder='The password to use to connect to the CloudSQL database']")
    public static WebElement password;

    @FindBy(how = How.XPATH, using = "//input [@type='radio' and @value='private']")
    public static WebElement instanceType;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='connectionName' and @class='MuiInputBase-input']")
    public static WebElement connectionName;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'Enter the internal')]")
    public static WebElement connectionNameError;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'Connection Name must')]")
    public static WebElement connectionNameFormatError;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='splitBy' and @class='MuiInputBase-input']")
    public static WebElement splitColumn;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='numSplits' and @class='MuiInputBase-input']")
    public static WebElement numberOfSplits;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='tableName' and @class='MuiInputBase-input']")
    public static WebElement sqlTableName;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='connectionTimeout' and @class='MuiInputBase-input']")
    public static WebElement connectionTimeout;

    @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
    public static WebElement closeButton;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-properties-validate-btn']")
    public static WebElement validateBtn;

    @FindBy(how = How.XPATH, using = "//*[@title=\"CloudSQL PostgreSQL\"]//following-sibling::div")
    public static WebElement cloudSqlPSqlProperties;

    @FindBy(how = How.XPATH, using = "//*[contains(text(),'Get Schema')]")
    public static WebElement getSchemaButton;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='CloudSQLPostgreSQL-preview-data-btn' and " +
      "@class='node-preview-data-btn ng-scope']")
    public static WebElement previewData;
}
