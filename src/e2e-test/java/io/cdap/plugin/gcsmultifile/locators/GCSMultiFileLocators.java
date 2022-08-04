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
package io.cdap.plugin.gcsmultifile.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * GCSMultiFile connector related locators.
 */
public class GCSMultiFileLocators {

  @FindBy(how = How.XPATH, using = "//input[@data-cy='referenceName']")
  public static WebElement referenceName;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='project']")
  public static WebElement projectId;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='path']")
  public static WebElement pathField;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='suffix']")
  public static WebElement pathSuffix;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='splitField']")
  public static WebElement splitField;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='toggle-No']")
  public static WebElement allowFlexible;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='select-format']")
  public static WebElement format;

  @FindBy(how = How.XPATH, using = "//a[@data-testid='close-config-popover']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='select-compressionCodec']")
  public static WebElement selectCodec;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='select-contentType']")
  public static WebElement selectContentType;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='delimiter']")
  public static WebElement delimiter;
}
