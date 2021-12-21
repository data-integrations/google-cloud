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
package io.cdap.plugin.pubsubsink.actions;

import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.pubsubsink.locators.PubSubLocators;
import org.openqa.selenium.By;

import java.util.UUID;

/**
 * PubSub Plugin testcase actions.
 */
public class PubSubActions {

  static {
    SeleniumHelper.getPropertiesLocators(CdfStudioLocators.class);
    SeleniumHelper.getPropertiesLocators(PubSubLocators.class);
  }

  public static void sinkPubSub() {
    CdfStudioLocators.sink.click();
    PubSubLocators.pubSubObject.click();
  }

  public static void pubsubProperties() {
    PubSubLocators.pubsubProperties.click();
  }

  public static void enterPubSubReferenceName() {
    PubSubLocators.pubsubReferenceName.sendKeys(UUID.randomUUID().toString());
  }

  public static void enterProjectID(String projectId) {
    SeleniumHelper.replaceElementValue(PubSubLocators.projectID, projectId);
  }

  public static void enterPubsubTopic(String pubSubTopic) {
  SeleniumHelper.sendKeys(PubSubLocators.pubsubTopic, pubSubTopic);
  }

  public static void selectFormat(String formatType) {
   PubSubLocators.format.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().
                                  findElement(By.xpath("//li[text()='" + formatType + "']")));
  }

  public static void enterMaximumBatchCount(String maximumBatchcount) {
    PubSubLocators.maximumBatchcount.sendKeys(maximumBatchcount);
  }

  public static void enterMaximumBatchSize(String maximumBatchSize) {
    PubSubLocators.maximumBatchSize.sendKeys(maximumBatchSize);
  }

  public static void enterPublishDelayThreshold(String publishDelayThreshold) {
    PubSubLocators.publishDelayThreshold.sendKeys(publishDelayThreshold);
  }

  public static void retryTimeOut(String retryTimeOut) {
    PubSubLocators.retryTimeout.sendKeys(retryTimeOut);
  }

  public static void errorThreshold(String errorThreshold) {
    PubSubLocators.errorThreshold.sendKeys(errorThreshold);
  }

 public  static void validate() {
  PubSubLocators.pluginValidationSuccessMsg.click();
 }

 public static void close() {
   PubSubLocators.closeButton.click(); }

  public static void clickPreviewData() {
    SeleniumHelper.waitAndClick(PubSubLocators.pubSubPreviewData);

  }
  public static void clickPluginProperties(String plugin) {
    SeleniumDriver.getDriver().findElement(
      By.xpath("//*[contains(@data-cy,'plugin-node-" + plugin + "')]//div[@class='node-metadata']/div[2]")).click();
  }
  public static void  connectSourceAndSink(String source, String sink) {
    SeleniumHelper.waitElementIsVisible(SeleniumDriver.getDriver().findElement(
      By.xpath("//*[contains(@data-cy,'plugin-node-" + sink + "')]")));
    SeleniumHelper.dragAndDrop(
      SeleniumDriver.getDriver().findElement(By.xpath("//*[contains(@class,'plugin-endpoint_" + source + "')]")),
      SeleniumDriver.getDriver().findElement(By.xpath("//*[contains(@data-cy,'plugin-node-" + sink + "')]")));
  }
}
