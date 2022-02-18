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
import io.cdap.plugin.spanner.locators.SpannerLocators;

import java.util.UUID;

/**
 * Spanner Plugin related step actions.
 */
public class SpannerActions {
    static {
        SeleniumHelper.getPropertiesLocators(SpannerLocators.class);
    }

    public static void enterProjectId(String projectId) {
        SeleniumHelper.replaceElementValue(SpannerLocators.spannerProjectId, projectId);
    }

    public static void enterReferenceName() {
        SpannerLocators.referenceName.sendKeys(UUID.randomUUID().toString());
    }

    public static void enterInstanceID(String instanceId) {
        SpannerLocators.spannerInstanceId.sendKeys(instanceId);
    }

    public static void enterImportQuery(String query) {
        SpannerLocators.importQuery.sendKeys(query);
    }

    public static void enterTableName(String tableName) {
        SpannerLocators.spannerTableName.sendKeys(tableName);
    }

    public static void enterDatabaseName(String databaseName) {
        SpannerLocators.spannerDatabaseName.sendKeys(databaseName);
    }

    public static void enterMaxPartitions(String maxPartitions) {
        SpannerLocators.maxPartitions.sendKeys(maxPartitions);
    }

    public static void enterPartitionSize(String partitionSize) {
        SpannerLocators.partitionSize.sendKeys(partitionSize);
    }

    public static void clickCloseButton()  {
        SpannerLocators.closeButton.click();
    }

    public static void getSchema() {
        SpannerLocators.getSchemaButton.click(); }

    public static void enterPrimaryKey(String primaryKey) {
        SpannerLocators.primaryKey.sendKeys(primaryKey);
    }

    public static void enterEncryptionKeyName(String cmek) {
        SpannerLocators.cmekKey.sendKeys(cmek);
    }

    public static void enterWriteBatchSize(String batchSize) {
        SpannerLocators.writeBatchSize.sendKeys(batchSize);
    }
}
