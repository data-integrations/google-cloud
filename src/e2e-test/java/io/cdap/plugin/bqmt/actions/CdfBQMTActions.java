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
package io.cdap.plugin.bqmt.actions;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.utils.CdapUtils;
import org.junit.Assert;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.PageFactory;

import java.io.IOException;
import java.util.UUID;

import static io.cdap.e2e.utils.ConstantsUtil.DATASET;
import static io.cdap.e2e.utils.ConstantsUtil.PROJECT_ID;

/**
 * Actions of BigQuery Multitable.
 */
public class CdfBQMTActions implements CdfHelper {

  public static io.cdap.plugin.bqmt.locators.CdfBQMTLocators cdfBQMTLocators;

  static {
    cdfBQMTLocators = PageFactory.initElements(SeleniumDriver.getDriver(),
                                               io.cdap.plugin.bqmt.locators.CdfBQMTLocators.class);
  }

  public static void enterReferenceName() {
    cdfBQMTLocators.bqmtReferenceName.sendKeys(UUID.randomUUID().toString());
  }

  public static String validateErrorColor(WebElement referenceName) {

    String color = referenceName.getCssValue("color");
    String[] hexValue = color.replace("rgba(", "").replace(")", "").split(",");

    int hexValue1 = Integer.parseInt(hexValue[0]);
    hexValue[1] = hexValue[1].trim();
    int hexValue2 = Integer.parseInt(hexValue[1]);
    hexValue[2] = hexValue[2].trim();
    int hexValue3 = Integer.parseInt(hexValue[2]);
    String actualColor = String.format("#%02x%02x%02x", hexValue1, hexValue2, hexValue3);
    return actualColor;
  }

  public static void labelNameValidation() throws Exception {
    String expectedTextInLabel = SeleniumHelper.readParameters("BQMT_Labelname");
    String actualTextInLabel = cdfBQMTLocators.bqmtLabel.getText();
    Assert.assertEquals(expectedTextInLabel, actualTextInLabel);
  }

  public static void referenceNameValidation() throws Exception {
    String expectedErrorMessage = CdapUtils.errorProp("errorMessageReference");
    String actualErrorMessage = cdfBQMTLocators.bqmtReferenceNameValidation.getText();
    WebElement referenceName = cdfBQMTLocators.bqmtReferenceNameValidation;
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = validateErrorColor(referenceName);
    String expectedColor = CdapUtils.errorProp("errorMessageColor");
    Assert.assertEquals(expectedColor, actualColor);
  }

  public static void dataSetValidation() throws Exception {
    WebElement dataSet = cdfBQMTLocators.bqmtDataSetValidation;
    String expectedErrorMessage = CdapUtils.errorProp("errorMessageDataset");
    String actualErrorMessage = dataSet.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = validateErrorColor(dataSet);
    String expectedColor = CdapUtils.errorProp("errorMessageColor");
    Assert.assertEquals(expectedColor, actualColor);
  }

  public static void temporaryBucketNameValidation() throws Exception {
    WebElement bucketName = cdfBQMTLocators.bqmtTemporaryBucketError;
    String expectedErrorMessage = CdapUtils.errorProp("errorMessageTemporaryBucket");
    String actualErrorMessage = bucketName.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = validateErrorColor(bucketName);
    String expectedColor = CdapUtils.errorProp("errorMessageColor");
    Assert.assertEquals(expectedColor, actualColor);
  }

  public static void enterProjectId() throws IOException {
    cdfBQMTLocators.bqmtProjectID.sendKeys(SeleniumHelper.readParameters(PROJECT_ID));
  }

  public static void enterDataset() throws IOException {
    cdfBQMTLocators.bqmtDataSet.sendKeys(SeleniumHelper.readParameters(DATASET));
  }

  public static void truncateTable() {
    cdfBQMTLocators.bqmtTruncateTable.click();
  }

  public static void serviceAccountTypeRadioButton() {
    cdfBQMTLocators.bqmtServiceAccountTypeFilePath.click();
  }

  public static void bqmtClickAgainProperties() {
    cdfBQMTLocators.bqmtClickAgainProperties.click();
  }

  public static void bqmtServiceAccountTypeJson() {
    cdfBQMTLocators.bqmtServiceAccountTypeJson.click();
  }

  public static void temporaryBucketName() {
    cdfBQMTLocators.bqmtTemporaryBucketName.sendKeys("cdf-athena-bq-bq-multitable");
  }

  public static void allowFlexibleSchemaInOutput() {
    cdfBQMTLocators.bqmtAllowflexibleschemasinOutput.click();
  }

  public static void closeButton() {
    cdfBQMTLocators.closeButton.click();
  }

  public static void bqmtProperties() {
    cdfBQMTLocators.bqmtProperties.click();
  }

  public static void validateBQMTLabel() {
    Assert.assertTrue(cdfBQMTLocators.bqmtLabel.isDisplayed());
  }

  public static void clearLabel() {
    cdfBQMTLocators.bqmtLabel.click();
    cdfBQMTLocators.bqmtLabel.sendKeys(Keys.COMMAND + "a");
    cdfBQMTLocators.bqmtLabel.sendKeys(Keys.BACK_SPACE);
  }

  public static void clickValidateButton() {
    io.cdap.plugin.bqmt.locators.CdfBQMTLocators.validateBtn.click();
  }

  public static void clickActionButton() {
    cdfBQMTLocators.clickActionButton.click();
  }

  public static void clickDuplicatebutton() {
    cdfBQMTLocators.clickDuplicatebutton.click();
  }

  public static void setTableSchemaTrue() {
    cdfBQMTLocators.bqmtUpdateTableSchemaTrue.click();
  }

  public static void clickComment() {
    cdfBQMTLocators.clickComment.click();
  }

  public static void addComment() throws IOException {
    cdfBQMTLocators.addComment.sendKeys(CdapUtils.pluginProp("COMMENT_BQMT"));
  }

  public static void saveComment() throws IOException {
    cdfBQMTLocators.enabledCommentButton.click();
  }

  public static void editComment() {
    cdfBQMTLocators.editComment.click();
  }

  public static void clickEdit() {
    cdfBQMTLocators.clickEdit.click();
  }

  public static void clickDelete() {
    cdfBQMTLocators.clickDelete.click();
  }

  public static void clearComments() {
    cdfBQMTLocators.addComment.click();
    cdfBQMTLocators.addComment.sendKeys(Keys.COMMAND + "a");
    cdfBQMTLocators.addComment.sendKeys(Keys.BACK_SPACE);

  }

  public static void clickHamburgerMenu() {
    cdfBQMTLocators.bqmthamburgermenu.click();
  }

  public static void clickHamburgerDelete() {
    cdfBQMTLocators.bqmthamburgerdelete.click();
  }

  public static void validateComment(String expected, String actual) {
    Assert.assertEquals(expected, actual);
  }
}
