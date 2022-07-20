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
package io.cdap.plugin.bigquerymultitable.actions;

import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.bigquerymultitable.locators.BQMTLocators;

import java.io.IOException;
import java.util.UUID;

/**
 * BQMT plugin step actions.
 */
public class BQMTActions {

  static {
    SeleniumHelper.getPropertiesLocators(BQMTLocators.class);
  }

  public static void enterBQMTDataset(String dataset) {
    ElementHelper.sendKeys(BQMTLocators.bqmtDataset, dataset);
  }

  public static void enterBQMTReferenceName() {
    ElementHelper.sendKeys(BQMTLocators.bqmtReferenceName, UUID.randomUUID().toString());
  }

  public static void enterProjectID(String projectId) {
    ElementHelper.replaceElementValue(BQMTLocators.projectID, projectId);
  }

  public static void close() {
    ElementHelper.clickOnElement(BQMTLocators.closeButton);
  }

  public static void clickTruncateTableSwitch() {
    ElementHelper.clickOnElement(BQMTLocators.truncateTableSwitch);
  }

  public static void clickAllowFlexibleSchemaSwitch() {
    ElementHelper.clickOnElement(BQMTLocators.bqmtAllowflexibleschemasinOutput);
  }

  public static void enterChunkSize(String chunkSize) {
    ElementHelper.sendKeys(BQMTLocators.chunkSize, chunkSize);
  }

  public static void enterSplitField(String splitField) {
    ElementHelper.sendKeys(BQMTLocators.splitField, splitField);
  }

  public static void enterReferenceName(String reference) {
    ElementHelper.sendKeys(BQMTLocators.bqmtReferenceName, reference);
  }

  public static void enterTemporaryBucketName(String bucket) throws IOException {
    ElementHelper.replaceElementValue(BQMTLocators.temporaryBucketName, bucket);
  }

  public static void selectUpdateTableSchema(String option) {
    ElementHelper.selectRadioButton(BQMTLocators.updateTableSchema(option));
  }

  public static void enterBQMTCmekProperty(String cmek) {
    ElementHelper.sendKeys(BQMTLocators.bqmtCmekKey, cmek);
  }
}
