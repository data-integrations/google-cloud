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
package io.cdap.plugin.pubsub.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * PubSub Related Locators.
 */
public class PubSubLocators {

  @FindBy(how = How.XPATH, using = "//input[@data-cy='referenceName']")
  public static WebElement pubSubReferenceName;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='project']")
  public static WebElement projectID;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='topic']")
  public static WebElement pubSubTopic;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='select-format']")
  public static WebElement format;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='messageCountBatchSize']")
  public static WebElement maximumBatchCount;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='requestThresholdKB']")
  public static WebElement maximumBatchSize;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='publishDelayThresholdMillis']")
  public static WebElement publishDelayThreshold;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='retryTimeoutSeconds']")
  public static WebElement retryTimeout;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='errorThreshold']")
  public static WebElement errorThreshold;

  @FindBy(how = How.XPATH, using = "//a[@data-testid='close-config-popover']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='cmekKey']")
  public static WebElement cmekKey;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='subscription']")
  public static WebElement subscription;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='numberOfReaders']")
  public static WebElement numberOfReaders;

  public static WebElement formatType(String formatType) {
    return SeleniumDriver.getDriver()
      .findElement(By.xpath("//li[@data-value='" + formatType + "' and text()='" + formatType + "']"));
  }

  public static WebElement selectedFormat(String format) {
    return SeleniumDriver.getDriver()
      .findElement(By.xpath("//*[@data-cy='select-format']/div[text()='" + format + "']"));
  }

  @FindBy(how = How.XPATH, using = "//span[contains(text(), \"Batch interval\")]//following-sibling::div//select[1]")
  public static WebElement batchTime;

  @FindBy(how = How.XPATH, using = "//span[contains(text(), \"Batch interval\")]//following-sibling::div//select[2]")
  public static WebElement timeSelect;

  @FindBy(how = How.XPATH, using = "//button[@data-testid='config-apply-close']")
  public static WebElement saveButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='pipeline-configure-modeless-btn']")
  public static WebElement configButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='tab-content-Pipeline config']")
  public static WebElement pipelineConfig;
}
