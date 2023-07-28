/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.bigquery.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * BigQuery Related Locators.
 */
public class BigQueryLocators {

  @FindBy(how = How.XPATH, using = "//input[@data-cy='referenceName']")
  public static WebElement bqmtReferenceName;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='project']")
  public static WebElement projectID;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='datasetProject']")
  public static WebElement datasetProject;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='dataset']")
  public static WebElement bqmtDataset;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='serviceFilePath']")
  public static WebElement serviceFilePath;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='bucket']")
  public static WebElement temporaryBucketName;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='gcsChunkSize']")
  public static WebElement chunkSize;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='splitField']")
  public static WebElement splitField;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='switch-allowFlexibleSchema']")
  public static WebElement bqmtAllowflexibleschemasinOutput;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='switch-truncateTable']")
  public static WebElement truncateTableSwitch;

  public static WebElement updateTableSchema(String option) {
    return SeleniumDriver.getDriver()
      .findElement(By.xpath("//*[@data-cy='allowSchemaRelaxation']//*[@value='" + option + "' and @type='radio']"));
  }

  @FindBy(how = How.XPATH, using = "//input[@data-cy='location']")
  public static WebElement location;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='cmekKey']")
  public static WebElement bqmtCmekKey;

  @FindBy(how = How.XPATH, using = "//a[@data-testid='close-config-popover']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='dedupeBy']//*[@data-cy='key']/input")
  public static WebElement dedupeBy;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='dedupeBy']//*[@data-cy='value']")
  public static WebElement orderBy;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='relationTableKey']//*[@data-cy='key']/input")
  public static WebElement tableKey;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='clusteringOrder']//*[@data-cy='key']/input")
  public static WebElement clusterOrder;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='sqlStatements']//*[@data-cy='key']/input")
  public static WebElement sqlStatement;
}
