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

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * PubSub Related Locators.
 */
public class PubSubLocators {

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-GooglePublisher-batchsink']")
  public static WebElement pubSubObject;

  @FindBy(how = How.XPATH, using = "//*[@title=\"Pub/Sub\"]//following-sibling::div")
  public static WebElement pubsubProperties;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='referenceName' and @class='MuiInputBase-input']")
  public static WebElement pubsubReferenceName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='project' and @class='MuiInputBase-input']")
  public static WebElement projectID;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='topic' and @class='MuiInputBase-input']")
  public static WebElement pubsubTopic;

  @FindBy(how = How.XPATH, using =  "//*[@data-cy='select-format']")
  public static WebElement format;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='messageCountBatchSize'and@class='MuiInputBase-input']")
  public static WebElement maximumBatchcount;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='requestThresholdKB' and @class='MuiInputBase-input']")
  public static WebElement maximumBatchSize;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='publishDelayThresholdMillis'and@class='MuiInputBase-input']")
  public static WebElement publishDelayThreshold;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='retryTimeoutSeconds' and @class='MuiInputBase-input']")
  public static WebElement retryTimeout;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='errorThreshold' and @class='MuiInputBase-input']")
  public static WebElement errorThreshold;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-properties-validate-btn']")
  public static WebElement pluginValidationSuccessMsg;

  @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//*[@title='Pub/Sub']")
  public  static WebElement toPubsub;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='get-schema-btn']//span[text()='Get Schema']")
  public static WebElement getSchemaLoadComplete;

  @FindBy(how = How.XPATH,
    using = "//*[@data-cy='GooglePublisher-preview-data-btn' and @class='node-preview-data-btn ng-scope']")
  public static WebElement pubSubPreviewData;
}
