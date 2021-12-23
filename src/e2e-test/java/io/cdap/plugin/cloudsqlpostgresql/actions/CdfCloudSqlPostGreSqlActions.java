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
package io.cdap.plugin.cloudsqlpostgresql.actions;

import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.cloudsqlpostgresql.locators.CdfCloudSqlPostGreSqlLocators;
import io.cdap.plugin.utils.CdapUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WindowType;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * cloudSqlPostGreSql connector related Step Actions.
 */

public class CdfCloudSqlPostGreSqlActions {

    static {
        SeleniumHelper.getPropertiesLocators(CdfCloudSqlPostGreSqlLocators.class);
        SeleniumHelper.getPropertiesLocators(CdfBigQueryPropertiesLocators.class);
    }

    public static void selectCloudSQLPostGreSQLSource() throws InterruptedException {
        SeleniumHelper.waitAndClick(CdfCloudSqlPostGreSqlLocators.cloudSqlPsqlSource);
    }

    public static void selectCloudSQLPostGreSQLSink() throws InterruptedException {
        CdfCloudSqlPostGreSqlLocators.sink.click();
        SeleniumHelper.waitAndClick(CdfCloudSqlPostGreSqlLocators.cloudSqlPsqlSink);
    }

    public static void clickCloudSqlPostGreSqlProperties() {
        CdfCloudSqlPostGreSqlLocators.cloudSqlPSqlProperties.click();
    }

    public static void clickValidateButton() {
        CdfCloudSqlPostGreSqlLocators.validateBtn.click();
    }

    public static void enterReferenceName(String reference) {
        CdfCloudSqlPostGreSqlLocators.referenceName.sendKeys(reference);
    }

    public static void enterDriverName(String driver) {
        CdfCloudSqlPostGreSqlLocators.driverName.sendKeys(driver);
    }

    public static void enterDefaultDriver(String driverNameValid) {
        SeleniumHelper.replaceElementValue(CdfCloudSqlPostGreSqlLocators.driverName, driverNameValid);
    }

    public static void enterDatabaseName(String database) {
        CdfCloudSqlPostGreSqlLocators.database.sendKeys(database);
    }

    public static void enterUserName(String username) {
        CdfCloudSqlPostGreSqlLocators.username.sendKeys(username);
    }

    public static void enterPassword(String password) {
        CdfCloudSqlPostGreSqlLocators.password.sendKeys(password);
    }

    public static void enterConnectionName(String connection) {
        CdfCloudSqlPostGreSqlLocators.connectionName.sendKeys(connection);
    }

    public static void closeButton() {
        CdfCloudSqlPostGreSqlLocators.closeButton.click();
    }

    public static void enterSplitColumn(String splitColumn) {
        CdfCloudSqlPostGreSqlLocators.splitColumn.sendKeys(splitColumn);
    }

    public static void enterNumberOfSplits(String numberOfSplits) {
        SeleniumHelper.replaceElementValue(CdfCloudSqlPostGreSqlLocators.numberOfSplits, numberOfSplits);
    }

    public static void replaceTableValue(String tableNameCS1) {
        SeleniumHelper.replaceElementValue(CdfCloudSqlPostGreSqlLocators.sqlTableName, tableNameCS1);
    }

    public static void enterImportQuery(String query) throws IOException, InterruptedException {
        CdfCloudSqlPostGreSqlLocators.importQuery.sendKeys(query);
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlPostGreSqlLocators.getSchemaButton, 30);
    }

    public static void enterBoundingQuery(String query) throws IOException, InterruptedException {
        CdfCloudSqlPostGreSqlLocators.boundingQuery.sendKeys(query);
    }

    public static void enterTableName(String table) {
        CdfCloudSqlPostGreSqlLocators.sqlTableName.sendKeys(table);
    }

    public static void enterConnectionTimeout(String connectionTimeout) {
        SeleniumHelper.replaceElementValue(CdfCloudSqlPostGreSqlLocators.connectionTimeout, connectionTimeout);
    }

    public static void clickPrivateInstance() {
        CdfCloudSqlPostGreSqlLocators.instanceType.click();
    }

    public static void getSchema() {
        CdfCloudSqlPostGreSqlLocators.getSchemaButton.click();
    }

    public static void clickPreviewData() {
        SeleniumHelper.waitElementIsVisible(CdfCloudSqlPostGreSqlLocators.previewData);
        CdfCloudSqlPostGreSqlLocators.previewData.click();
    }

    public static void replaceSplitValue(String numberOfSplits) throws IOException {
        SeleniumHelper.replaceElementValue(CdfCloudSqlPostGreSqlLocators.numberOfSplits, numberOfSplits);
        }
    }

