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
package io.cdap.plugin.gcsmultifile.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * GCSMultiFile connector related locators.
 */
public class GCSMultiFileLocators {

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"referenceName\" and @class=\"MuiInputBase-input\"]")
  public static WebElement referenceName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"project\" and @class=\"MuiInputBase-input\"]")
  public static WebElement projectId;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"path\" and @class=\"MuiInputBase-input\"]")
  public static WebElement pathField;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"suffix\" and @class=\"MuiInputBase-input\"]")
  public static WebElement pathSuffix;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"splitField\" and @class=\"MuiInputBase-input\"]")
  public static WebElement splitField;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"toggle-No\"]")
  public static WebElement allowFlexible;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"select-format\"]")
  public static WebElement format;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-GCSMultiFiles-batchsink']")
  public static WebElement gcsMultiFileObject;

  @FindBy(how = How.XPATH, using = "//*[@title='GCS Multi File']")
  public static WebElement toGcsMultifile;

  @FindBy(how = How.XPATH, using = "//*[@title='GCS Multi File']//following-sibling::div")
  public static WebElement gcsMultifileProperties;

  @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='datasetProject' and @class='MuiInputBase-input']")
  public static WebElement datasetProjectId;

  @FindBy(how = How.XPATH, using = "//*[text()='Source ']")
  public static WebElement source;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='valium-banner-hydrator']")
  public static WebElement pipelineSaveSuccessBanner;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='select-compressionCodec']")
  public static WebElement selectCodec;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"select-contentType\"]")
  public static WebElement selectContentType;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='get-schema-btn']//span[text()='Get Schema']")
  public static WebElement getSchemaLoadComplete;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"delimiter\" and @class=\"MuiInputBase-input\"]")
  public static WebElement delimiter;
}
