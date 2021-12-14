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
package io.cdap.plugin.spanner.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * All Spanner Locators.
 */

public class CdfSpannerLocators {

  @FindBy(how = How.XPATH, using = "//*[@data-cy='referenceName' and @class='MuiInputBase-input']")
  public static WebElement referenceName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"plugin-Spanner-batchsource\"]")
  public static WebElement spannerBucket;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'Select one')]")
  public static WebElement format;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='project' and @class='MuiInputBase-input']")
  public static WebElement spannerProjectId;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='serviceFilePath' and @class='MuiInputBase-input']")
  public static WebElement spannerFilePath;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-properties-validate-btn']")
  public static WebElement validateButton;

  @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//*[@title=\"Spanner\"]//following-sibling::div")
  public static WebElement spannerProperties;

  @FindBy(how = How.XPATH, using = "//*[contains(text(),'Get Schema')]")
  public static WebElement getSchemaButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='Spanner-preview-data-btn']")
  public static WebElement spannerPreviewDataButton;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='instance']")
  public static WebElement spannerInstanceId;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='database']")
  public static WebElement spannerDatabaseeName;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='table']")
  public static WebElement spannerTableName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='get-schema-btn']//span[text()='Get Schema']")
  public static WebElement getSchemaLoadComplete;

  @FindBy(how = How.XPATH,
    using = "//*[@data-cy='Spanner-preview-data-btn' and @class='node-preview-data-btn ng-scope']")
  public static WebElement spannerPreviewData;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='importQuery']//textarea")
  public static WebElement importQuery;
}
