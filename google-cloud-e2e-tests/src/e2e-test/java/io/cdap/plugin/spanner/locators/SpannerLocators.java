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
package io.cdap.plugin.spanner.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Spanner plugin related element locators.
 */

public class SpannerLocators {

  @FindBy(how = How.XPATH, using = "//input[@data-cy='referenceName']")
  public static WebElement referenceName;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='project']")
  public static WebElement spannerProjectId;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='instance']")
  public static WebElement spannerInstanceId;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='database']")
  public static WebElement spannerDatabaseName;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='table']")
  public static WebElement spannerTableName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='importQuery']//textarea")
  public static WebElement importQuery;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='get-schema-btn']//span[text()='Get Schema']")
  public static WebElement getSchemaButton;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='maxPartitions']")
  public static WebElement maxPartitions;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='partitionSizeMB']")
  public static WebElement partitionSize;

  @FindBy(how = How.XPATH, using = "//a[@data-testid='close-config-popover']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='key']/input")
  public static WebElement primaryKey;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='cmekKey']")
  public static WebElement cmekKey;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='batchSize']")
  public static WebElement writeBatchSize;
}
