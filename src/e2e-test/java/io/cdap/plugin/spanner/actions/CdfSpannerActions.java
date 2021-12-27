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
package io.cdap.plugin.spanner.actions;

import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.spanner.locators.CdfSpannerLocators;
import java.io.IOException;
import java.util.UUID;

/**
 * Spanner Connector related Actions.
 */
public class CdfSpannerActions {
    static {
        SeleniumHelper.getPropertiesLocators(CdfSpannerLocators.class);
    }

    public static void spannerProperties() {
        CdfSpannerLocators.spannerProperties.click();
    }

    public static void enterProjectId(String projectId) throws IOException {
        SeleniumHelper.replaceElementValue(CdfSpannerLocators.spannerProjectId, projectId);
    }

    public static void enterReferenceName() {
        CdfSpannerLocators.referenceName.sendKeys(new CharSequence[]{UUID.randomUUID().toString()});
    }

    public static void enterInstanceID(String istanceId) {
        CdfSpannerLocators.spannerInstanceId.sendKeys(istanceId);
    }

    public static void enterImportQuery(String query) {
        CdfSpannerLocators.importQuery.sendKeys(query);
    }

    public static void clickValidateButton() {
        CdfSpannerLocators.validateButton.click();
    }

    public static void enterTableName(String tableName) {
        CdfSpannerLocators.spannerTableName.sendKeys(tableName);
    }

    public static void enterDatabaseName(String databaseName) {
        CdfSpannerLocators.spannerDatabaseName.sendKeys(databaseName);
    }

    public static void clickSpannerPreviewData() {
        SeleniumHelper.waitAndClick(CdfSpannerLocators.spannerPreviewData);
    }

    public static void clickGCSPreviewData() {
        SeleniumHelper.waitAndClick(CdfSpannerLocators.gcsPreviewData);
    }

    public static void selectSpanner() throws InterruptedException {
        SeleniumHelper.waitAndClick(CdfSpannerLocators.spannerBatchSource);
    }

    public static void clickPreviewPropertiesTab() {
        CdfSpannerLocators.previewPropertiesTab.click();
    }

    public static void closeButton()  {
        CdfSpannerLocators.closeButton.click();
    }

    public static void getSchema() {
        CdfSpannerLocators.getSchemaButton.click(); }
}
