/*
 * Copyright © 2022 Cask Data, Inc.
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

import io.cdap.e2e.utils.ElementHelper;
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
    ElementHelper.replaceElementValue(SpannerLocators.spannerProjectId, projectId);
  }

  public static void enterReferenceName() {
    ElementHelper.sendKeys(SpannerLocators.referenceName, UUID.randomUUID().toString());
  }

  public static void enterInstanceID(String instanceId) {
    ElementHelper.sendKeys(SpannerLocators.spannerInstanceId, instanceId);
  }

  public static void enterImportQuery(String query) {
    // ElementHelper.sendKeys is avoided as importQuery textarea has style=opacity: 0
    // Selenium waiting for visibility throws exception.
    SpannerLocators.importQuery.sendKeys(query);
  }

  public static void enterTableName(String tableName) {
    ElementHelper.sendKeys(SpannerLocators.spannerTableName, tableName);
  }

  public static void enterDatabaseName(String databaseName) {
    ElementHelper.sendKeys(SpannerLocators.spannerDatabaseName, databaseName);
  }

  public static void enterMaxPartitions(String maxPartitions) {
    ElementHelper.sendKeys(SpannerLocators.maxPartitions, maxPartitions);
  }

  public static void enterPartitionSize(String partitionSize) {
    ElementHelper.sendKeys(SpannerLocators.partitionSize, partitionSize);
  }

  public static void clickCloseButton() {
    ElementHelper.clickOnElement(SpannerLocators.closeButton);
  }

  public static void getSchema() {
    ElementHelper.clickOnElement(SpannerLocators.getSchemaButton);
  }

  public static void enterPrimaryKey(String primaryKey) {
    ElementHelper.sendKeys(SpannerLocators.primaryKey, primaryKey);
  }

  public static void enterEncryptionKeyName(String cmek) {
    ElementHelper.sendKeys(SpannerLocators.cmekKey, cmek);
  }

  public static void enterWriteBatchSize(String batchSize) {
    ElementHelper.sendKeys(SpannerLocators.writeBatchSize, batchSize);
  }
}
