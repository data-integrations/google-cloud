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
package io.cdap.plugin.pubsub.actions;

import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.pubsub.locators.PubSubLocators;
import org.junit.Assert;
import org.openqa.selenium.WebElement;

import java.util.UUID;

/**
 * PubSub plugin step actions.
 */
public class PubSubActions {

  static {
    SeleniumHelper.getPropertiesLocators(CdfStudioLocators.class);
    SeleniumHelper.getPropertiesLocators(PubSubLocators.class);
  }

  public static void enterPubSubReferenceName() {
    PubSubLocators.pubSubReferenceName.sendKeys(UUID.randomUUID().toString());
  }

  public static void enterProjectID(String projectId) {
    SeleniumHelper.replaceElementValue(PubSubLocators.projectID, projectId);
  }

  public static void enterPubSubTopic(String pubSubTopic) {
    SeleniumHelper.sendKeys(PubSubLocators.pubSubTopic, pubSubTopic);
  }

  public static void selectFormat(String formatType) {
    PubSubLocators.format.click();
    WebElement formatTypeListOption = PubSubLocators.formatType(formatType);
    ElementHelper.clickOnElement(formatTypeListOption);
    WaitHelper.waitForElementToBeHidden(formatTypeListOption);
    Assert.assertTrue(formatType + " format should be selected",
                      ElementHelper.isElementDisplayed(PubSubLocators.selectedFormat(formatType)));
  }

  public static void enterMaximumBatchCount(String maximumBatchcount) {
    SeleniumHelper.replaceElementValue(PubSubLocators.maximumBatchCount, maximumBatchcount);
  }

  public static void enterMaximumBatchSize(String maximumBatchSize) {
    SeleniumHelper.replaceElementValue(PubSubLocators.maximumBatchSize, maximumBatchSize);
  }

  public static void enterPublishDelayThreshold(String publishDelayThreshold) {
    SeleniumHelper.replaceElementValue(PubSubLocators.publishDelayThreshold, publishDelayThreshold);
  }

  public static void enterRetryTimeOut(String retryTimeOut) {
    SeleniumHelper.replaceElementValue(PubSubLocators.retryTimeout, retryTimeOut);
  }

  public static void enterErrorThreshold(String errorThreshold) {
    SeleniumHelper.replaceElementValue(PubSubLocators.errorThreshold, errorThreshold);
  }

  public static void close() {
    PubSubLocators.closeButton.click();
  }

  public static void enterEncryptionKeyName(String cmek) {
    PubSubLocators.cmekKey.sendKeys(cmek);
  }

  public static void enterSubscription(String subscription) {
    PubSubLocators.subscription.sendKeys(subscription);
  }

  public static void enterNumberOfReaders(String numberOfReaders) {
    SeleniumHelper.replaceElementValue(PubSubLocators.numberOfReaders, numberOfReaders);
  }

}
