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
package io.cdap.plugin.file.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * File Connector Locators.
 */

public class CdfFileLocators {

  @FindBy(how = How.XPATH, using = "//*[@placeholder='Name used to identify this source for lineage']")
  public static WebElement referenceName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'referenceName')]")
  public static WebElement referenceNameerror;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='project' and @class='MuiInputBase-input']")
  public static WebElement projectID;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='path' and @class='MuiInputBase-input']")
  public static WebElement gcsPath;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='path' and @class='MuiInputBase-input']")
  public static WebElement filePath;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'path')]")
  public static WebElement filePatherror;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'format')]")
  public static WebElement fileFormaterror;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='sampleSize' and @class='MuiInputBase-input']")
  public static WebElement samplesize;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'Select one')]")
  public static WebElement format;

  @FindBy(how = How.XPATH, using = "//*[@class='plugin-comments-wrapper ng-scope']")
  public static WebElement commentWindow;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'Comment')]")
  public static WebElement commentButton;

  @FindBy(how = How.XPATH, using = "//*[@placeholder='Add a comment']")
  public static WebElement addcommentWindow;

  @FindBy(how = How.XPATH, using = "//*[@class='comments-wrapper ng-scope']//child::div[1]/*[1]")
  public static WebElement commentWinFileProgerrties;

  @FindBy(how = How.XPATH, using = "//*[@class='MuiButtonBase-root MuiIconButton-root MuiIconButton-sizeSmall']")
  public static WebElement editComment;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'Edit')]")
  public static WebElement editCommentButton;

  @FindBy(how = How.XPATH, using = "//*[@id=\"menu-list-grow\"]/li[2]")
  public static WebElement editDeleteButton;

  @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//*[@title=\"GCS\"]//following-sibling::div")
  public static WebElement gcsProperties;

  @FindBy(how = How.XPATH, using = "//*[@title=\"File\"]//following-sibling::div")
  public static WebElement fileProperties;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='switch-skipHeader']")
  public static WebElement skipHeader;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'Get Schema')]")
  public static WebElement getSchemaButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"plugin-File-batchsource\"]")
  public static WebElement fileBucket;

  @FindBy(how = How.XPATH, using = "//*[contains(@class,'plugin-endpoint_File')]")
  public static WebElement fromFile;

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

}

