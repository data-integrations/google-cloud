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

    @FindBy(how = How.XPATH, using = "//*[@class='plugin-comments-wrapper ng-scope']")
    public static WebElement clickComment;

    @FindBy(how = How.XPATH, using = "(//div[contains(@class,'MuiPaper-rounded')])[4]/div/textarea[1]")
    public static WebElement addComment;

    @FindBy(how = How.XPATH, using = "(//div[contains(@class,'MuiPaper-rounded')])[4]/div[2]/div/p")
    public static WebElement validateComment;

    @FindBy(how = How.XPATH, using = "(//div[contains(@class,'MuiPaper-rounded')])[3]/div[2]/div/p")
    public static WebElement validateSinkComment;

    @FindBy(how = How.XPATH, using = "(//*[contains(text(), 'Comment')])[2]")
    public static WebElement saveComment;

    @FindBy(how = How.XPATH, using = "(//*[contains(@class,'MuiIconButton-sizeSmall') and @tabindex='0'])")
    public static WebElement editComment;

    @FindBy(how = How.XPATH, using = "(//*[@id='menu-list-grow']//child::li)[1]")
    public static WebElement clickEdit;

    @FindBy(how = How.XPATH, using = "(//*[@id='menu-list-grow']//child::li)[2]")
    public static WebElement clickDelete;

    @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
    public static WebElement closeButton;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-properties-validate-btn']")
    public static WebElement validateBtn;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'referenceName')]")
    public static WebElement cldMysqlReferenceNameValidation;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'database')]")
    public static WebElement cldMysqlDatabaseNameValidation;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'connectionName')]")
    public static WebElement cldMysqlConnectionNameValidation;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'importQuery')]")
    public static WebElement cldMysqlImportQueryValidation;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'tableName')]")
    public static WebElement cldMysqlTableNameValidation;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'jdbcPluginName')]")
    public static WebElement cldMysqlDriverNameValidation;

    @FindBy(how = How.XPATH, using = "//*[@title=\"CloudSQL PostgreSQL\"]//following-sibling::div")
    public static WebElement cloudSqlPSqlProperties;

    @FindBy(how = How.XPATH, using = "//*[contains(text(),'Get Schema')]")
    public static WebElement getSchemaButton;

    @FindBy(how = How.XPATH, using = "//*[@class=\"btn pipeline-action-btn pipeline-actions-btn\"]")
    public static WebElement actionButton;

    @FindBy(how = How.XPATH, using = "//*[@class=\"btn btn-primary save-button\"]")
    public static WebElement pipelineSave;

    @FindBy(how = How.XPATH, using = "//*[contains(text(),'Duplicate')]")
    public static WebElement actionDuplicateButton;

    // Later below two locator should be deled when code will re-factor
    @FindBy(how = How.XPATH, using = "//*[@data-cy='serviceAccountJSON' and @class='MuiInputBase-input']")
    public static WebElement bigQueryServiceAccountJSON;

    @FindBy(how = How.XPATH, using = "//input [@type='radio' and @value='JSON']")
    public static WebElement bigQueryJson;

    @FindBy(how = How.XPATH, using = "(//*[@data-cy='plugin-properties-errors-found']")
    public static WebElement getSchemaStatus;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-BigQueryTable-batchsource']")
    public static WebElement selectBigQuerySource;

    @FindBy(how = How.XPATH, using = "//*[@placeholder='Add a comment']")
    public static WebElement addCommentSink;

    @FindBy(how = How.XPATH, using = "//*[@data-cy='CloudSQLPostgreSQL-preview-data-btn' and " +
      "@class='node-preview-data-btn ng-scope']")
    public static WebElement previewData;
}
