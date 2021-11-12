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
package io.cdap.plugin.bqmt.stepsdesign;

import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.bqmt.actions.CdfBQMTActions;
import io.cdap.plugin.bqmt.locators.CdfBQMTLocators;
import io.cdap.plugin.utils.CdapUtils;
import io.cucumber.java.en.Then;
import org.junit.Assert;

import java.io.IOException;

import static io.cdap.plugin.utils.GCConstants.COMMENT_BQMT;
import static io.cdap.plugin.utils.GCConstants.INVALID_TESTDATA;

/**
 * BQMT DesignTime testcases.
 */
public class BQMTDesignTime implements CdfHelper {

  @Then("Open BQMT Properties")
  public void openBQMTProperties() {
    CdfBQMTActions.bqmtProperties();
    SeleniumHelper.waitElementIsVisible(CdfBQMTLocators.bqmtPropertyHeader);
  }

  @Then("Verify Reference Field")
  public void verifyReferenceField() throws Exception {
    CdfBQMTActions.referenceNameValidation();
  }

  @Then("Validate Pipeline")
  public void validatePipeline() {
    CdfBQMTActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfBQMTLocators.validateMultiErrorMsg);
  }

  @Then("Close the BQMT Properties")
  public void closeTheBQMTProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Verify DataSet Field")
  public void verifyDataSetField() throws Exception {
    CdfBQMTActions.dataSetValidation();
  }

  @Then("Enter Reference Name & DataSet Fields with Invalid Test Data")
  public void enterTheBQMTwithInvalidTestData() {
    CdfBQMTActions.bqmtProperties();
    CdfBQMTLocators.bqmtReferenceName.sendKeys(INVALID_TESTDATA);
    CdfBQMTLocators.bqmtDataSet.sendKeys(INVALID_TESTDATA);
    CdfBQMTActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfBQMTLocators.validateMultiErrorMsg);
  }

  @Then("Verify Reference Name & DataSet Fields with Invalid Test Data")
  public void verifyTheBQMTwithInvalidTestData() {
    Assert.assertTrue(CdfBQMTLocators.bqmtReferencenameError.isDisplayed());
    Assert.assertTrue(CdfBQMTLocators.bqmtDataSetError.isDisplayed());
  }

  @Then("Enter Temporary Bucket Field with Invalid Test Data and Verify the Error Message")
  public void enterTheBQMTTemporaryBucketwithInvalidTestData() throws Exception {
    CdfBQMTActions.bqmtProperties();
    CdfBQMTActions.enterReferenceName();
    CdfBQMTActions.enterDataset();
    CdfBQMTLocators.bqmtTemporaryBucketName.sendKeys(INVALID_TESTDATA);
    CdfBQMTActions.clickValidateButton();
    CdfBQMTActions.temporaryBucketNameValidation();
    SeleniumHelper.waitElementIsVisible(CdfBQMTLocators.validateSingleErrorMsg);
  }

  @Then("Add and Save Comments")
  public void addComments() throws IOException {
    CdfBQMTActions.bqmtProperties();
    SeleniumHelper.waitElementIsVisible(CdfBQMTLocators.bqmtPropertyHeader);
    CdfBQMTActions.clickComment();
    CdfBQMTActions.addComment();
    CdfBQMTActions.saveComment();
  }

  @Then("Edit Comments")
  public void editComments() throws IOException {
    CdfBQMTActions.editComment();
    CdfBQMTActions.clickEdit();
    SeleniumHelper.waitElementIsVisible(CdfBQMTLocators.enabledCommentButton);
    CdfBQMTActions.clearComments();
    SeleniumHelper.waitElementIsVisible(CdfBQMTLocators.disabledCommentButton);
    CdfBQMTActions.addComment();
    SeleniumHelper.waitElementIsVisible(CdfBQMTLocators.enabledCommentButton);
    CdfBQMTActions.saveComment();
  }

  @Then("Delete Comments")
  public void deleteComments() {
    CdfBQMTActions.editComment();
    CdfBQMTActions.clickDelete();
  }

  @Then("Validate Comment")
  public void validateComment() throws IOException {
    CdfBQMTActions.validateComment(CdapUtils.pluginProp(COMMENT_BQMT), CdfBQMTLocators.addComment.getText());
  }
}
