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
package io.cdap.plugin.bqmt.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Locators of BigQuery Multitable.
 */
public class CdfBQMTLocators {

  @FindBy(how = How.XPATH, using = "//input[@type='text']")
  public static WebElement bqmtLabel;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='referenceName' and @class='MuiInputBase-input']")
  public static WebElement bqmtReferenceName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'referenceName')]")
  public static WebElement bqmtReferenceNameValidation;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='project' and @class='MuiInputBase-input']")
  public static WebElement bqmtProjectID;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='dataset' and @class='MuiInputBase-input']")
  public static WebElement bqmtDataSet;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'dataset')]")
  public static WebElement bqmtDataSetValidation;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='switch-truncateTable']")
  public static WebElement bqmtTruncateTable;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'File Path')]")
  public static WebElement bqmtServiceAccountTypeFilePath;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'JSON') and contains(@class,'MuiTypography-root')]")
  public static WebElement bqmtServiceAccountTypeJson;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='bucket' and @class='MuiInputBase-input']")
  public static WebElement bqmtTemporaryBucketName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='switch-allowFlexibleSchema']")
  public static WebElement bqmtAllowflexibleschemasinOutput;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'True') and contains(@class,'MuiTypography-root')]")
  public static WebElement bqmtUpdateTableSchemaTrue;

  @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='node-error-count' and @class='ng-binding']")
  public static WebElement bqmtProperties;

  @FindBy(how = How.XPATH, using = "//*[@class='node-version ng-binding']")
  public static WebElement bqmtClickAgainProperties;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='referenceName']/following-sibling::div")
  public static WebElement bqmtReferencenameError;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='dataset']/following-sibling::div")
  public static WebElement bqmtDataSetError;

  @FindBy(how = How.XPATH, using = "//*[@class='btn pipeline-action-btn pipeline-actions-btn']")
  public static WebElement clickActionButton;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'Duplicate')]")
  public static WebElement clickDuplicatebutton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='bucket']/following-sibling::div")
  public static WebElement bqmtTemporaryBucketError;

  @FindBy(how = How.XPATH, using = "//*[@class='MuiSvgIcon-root jss59 jss60']")
  public static WebElement clickComment;

  @FindBy(how = How.XPATH, using = "//*[@placeholder='Add a comment']")
  public static WebElement addComment;

  @FindBy(how = How.XPATH, using = "(//*[contains(@class,'MuiIconButton-sizeSmall') and @tabindex='0'])")
  public static WebElement editComment;

  @FindBy(how = How.XPATH, using = "(//*[@id='menu-list-grow']//child::li)[1]")
  public static WebElement clickEdit;

  @FindBy(how = How.XPATH, using = "(//*[@id='menu-list-grow']//child::li)[2]")
  public static WebElement clickDelete;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='hamburgermenu-BigQueryMultiTable-batchsink-0-toggle']")
  public static WebElement bqmthamburgermenu;

  @FindBy(how = How.XPATH, using = "//*[@class='menu-content-action menu-content-delete']")
  public static WebElement bqmthamburgerdelete;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-properties-validate-btn']")
  public static WebElement validateBtn;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'BigQuery Multi Table Properties')]")
  public static WebElement bqmtPropertyHeader;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'errors found.')]")
  public static WebElement validateMultiErrorMsg;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'error found')]")
  public static WebElement validateSingleErrorMsg;

  @FindBy(how = How.XPATH, using =
    "//*[@class='MuiButtonBase-root MuiButton-root MuiButton-contained" +
      " MuiButton-containedPrimary Mui-disabled Mui-disabled']")
  public static WebElement disabledCommentButton;

  @FindBy(how = How.XPATH, using =
    "//*[@class='MuiButtonBase-root MuiButton-root MuiButton-contained MuiButton-containedPrimary']")
  public static WebElement enabledCommentButton;
}
