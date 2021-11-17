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
package io.cdap.plugin.file.stepsdesign;

import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.file.actions.CdfFileActions;
import io.cdap.plugin.file.locators.CdfFileLocators;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;

import java.io.IOException;

/**
 * File Connector related Stepdesigen.
 **/

public class FileConnector implements CdfHelper {
  static int i = 0;
  static boolean alert = false;

  @When("Source is File bucket")
  public void sourceIsfileBucket() throws InterruptedException {
    CdfFileActions.selectFile();
  }

  @Then("Link Source-File and Sink-BigQuery to establish connection")
  public void linkSourceAndSinkToEstablishConnection() throws InterruptedException {
    Thread.sleep(2000);
    SeleniumHelper.dragAndDrop(CdfFileLocators.fromFile, CdfStudioLocators.toBigQiery);
  }

  @Then("Close the File Properties")
  public void closeTheFileProperties() {
    CdfFileActions.closeButton();
  }

  @Then("Enter the File Properties with {string} File bucket and {string} file format")
  public void enterTheFileProperties(String filepath, String fileFormat)
    throws InterruptedException, IOException {
    CdfFileActions.fileProperties();
    CdfGcsActions.enterReferenceName();
    if (!filepath.equalsIgnoreCase("")) {
      CdfFileActions.enterFileBucket(filepath);
    }
    if (!fileFormat.equalsIgnoreCase("")) {
      CdfGcsActions.selectFormat(fileFormat);
    }
    CdfFileActions.skipHeader();
    CdfFileActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.getSchemaButton, 1L);
  }

  @Then("Enter the File Properties with {string} fileBucket and {string} fileFormat without reference name")
  public void enterTheFilePropertiesWithFileBucketWithOutReferenceName(String filepath, String fileFormat)
    throws InterruptedException, IOException {
    CdfFileActions.fileProperties();
    if (!filepath.equalsIgnoreCase("")) {
      CdfFileActions.enterFileBucket(filepath);
    }
    if (!fileFormat.equalsIgnoreCase("")) {
      CdfGcsActions.selectFormat(fileFormat);
    }
    CdfFileActions.skipHeader();
    CdfFileActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.getSchemaButton, 1L);
  }

  @Then("Verify Reference Name Field")
  public void verifyReferenceNameField() {
  }

  @Then("Verify Path Field")
  public void verifyPathField() {
  }

  @Then("Validate the Reference Name with blank value")
  public void validateTheReferenceNameWithBlankValue() {
    String expectedErrorMessage = "Required property 'referenceName' has no value.";
    String actualErrorMessage = CdfFileLocators.referenceNameerror.getText();
    String color = CdfFileLocators.referenceNameerror.getCssValue("color");
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    Assert.assertEquals("rgba(164, 4, 3, 1)", color);
  }

  @Then("Validate the Reference Name with blank Path value")
  public void validateTheReferenceNameWithBlankPathValue() {
    String expectedErrorMessage = "Required property 'referenceName' has no value.";
    String actualErrorMessage = CdfFileLocators.referenceNameerror.getText();
    String color = CdfFileLocators.referenceNameerror.getCssValue("color");
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    Assert.assertEquals("rgba(164, 4, 3, 1)", color);
  }

  @Then("Validate the Path Name with blank Path value")
  public void validateThePathNameWithBlankPathValue() {
    String expectedErrorMessage = "Required property 'path' has no value.";
    String actualErrorMessage = CdfFileLocators.filePatherror.getText();
    String color = CdfFileLocators.filePatherror.getCssValue("color");
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    Assert.assertEquals("rgba(164, 4, 3, 1)", color);
  }

  @Then("Validate the File Format with blank value")
  public void validateTheFileFormatWithBlankValue() {
    String expectedErrorMessage = "Required property 'format' has no value.";
    String actualErrorMessage = CdfFileLocators.fileFormaterror.getText();
    String color = CdfFileLocators.fileFormaterror.getCssValue("color");
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    Assert.assertEquals("rgba(164, 4, 3, 1)", color);
  }

  @Then("Verify Schema in output")
  public void verifySchemaInOutput() {
    CdfFileActions.schemaValidation();
    CdfFileActions.assertionverification();
  }

  @Then("validate the pipline staus in faild")
  public void validateThePiplineStausInFaild() {
    WebElement status = SeleniumDriver.getDriver().
      findElement(By.xpath("//*[@data-cy='Succeeded' or @data-cy='Failed']"));
    if (status.isDisplayed()) {
      String failed = status.getText();
      Assert.assertEquals(failed, "Failed");
    }
  }

  @Then("Open the File Properties")
  public void openTheFileProperties() throws InterruptedException, IOException {
    CdfFileActions.fileProperties();
  }

  @Then("Add comments in comment window")
  public void addCommentsInCommentWindow() {
  }

  @Then("Open the File Properties and add comments")
  public void openTheFilePropertiesAndAddComments() throws InterruptedException, IOException {
    CdfFileActions.fileProperties();
    CdfFileActions.commentWindow();
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.commentWindow, 1L);
    CdfFileActions.addcommentWindow();
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.addcommentWindow, 1L);
    CdfFileActions.commentButton();
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.commentButton, 1L);
  }

  @Then("Veriy the Comments over File Propertie window")
  public void veriyTheCommentsOverFilePropertieWindow() throws InterruptedException, IOException {
    CdfFileActions.commentWinFileProgerrties();
    CdfFileActions.editComment();
  }

  @Then("Create Duplicate pileline")
  public void createDuplicatePileline() throws InterruptedException {
    CdfFileActions.actionButton();
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.actionDuplicateButton, 1L);
    CdfFileActions.actionDuplicateButton();
    CdfStudioActions.pipelineDeploy();
  }

  @Then("Veriy the Edit Comments over File Propertie window")
  public void veriyTheEditCommentsOverFilePropertieWindow() throws InterruptedException, IOException {
    CdfFileActions.commentWinFileProgerrties();
    CdfFileActions.editComment();
    CdfFileActions.editCommentButton();
    CdfFileLocators.addcommentWindow.sendKeys(Keys.COMMAND + "a");
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.addcommentWindow, 1L);
    CdfFileLocators.addcommentWindow.sendKeys(Keys.DELETE);
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.addcommentWindow, 1L);
    CdfFileLocators.addcommentWindow.sendKeys("Test for Demo");
    CdfFileLocators.commentButton.click();
  }

  @Then("Veriy the Delete Comments over File Propertie window")
  public void veriyTheDeleteCommentsOverFilePropertieWindow() throws InterruptedException, IOException {
    CdfFileActions.commentWinFileProgerrties();
    CdfFileActions.editComment();
    CdfFileActions.editCommentButton();
    CdfFileLocators.addcommentWindow.sendKeys(Keys.COMMAND + "a");
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.addcommentWindow, 1L);
    CdfFileLocators.addcommentWindow.sendKeys(Keys.DELETE);
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.addcommentWindow, 1L);
    CdfFileLocators.addcommentWindow.sendKeys("Test for Demo");
    CdfFileLocators.commentButton.click();
  }

  @Then("Delete the Comment window")
  public void deleteTheCommentWindow() throws InterruptedException, IOException {
    CdfFileActions.editComment();
    CdfFileLocators.editDeleteButton.click();
  }

  @Then("Enter the BigQuery Properties for table {string} with JSON")
  public void enterTheBigQueryPropertiesForTable(String tableProp) throws InterruptedException, IOException {
     CdfFileActions.enterBigQueryPropertiesWithJosn(tableProp);
  }

}

