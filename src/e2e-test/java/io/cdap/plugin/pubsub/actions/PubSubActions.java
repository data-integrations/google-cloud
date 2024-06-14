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

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfSchemaLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.pubsub.locators.PubSubLocators;
import org.openqa.selenium.support.ui.Select;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * PubSub plugin step actions.
 */
public class PubSubActions {

  static {
    SeleniumHelper.getPropertiesLocators(CdfStudioLocators.class);
    SeleniumHelper.getPropertiesLocators(PubSubLocators.class);
  }

  public static void enterPubSubReferenceName() {
    ElementHelper.sendKeys(PubSubLocators.pubSubReferenceName, UUID.randomUUID().toString());
  }

  public static void enterProjectID(String projectId) {
    ElementHelper.replaceElementValue(PubSubLocators.projectID, projectId);
  }

  public static void enterPubSubTopic(String pubSubTopic) {
    ElementHelper.sendKeys(PubSubLocators.pubSubTopic, pubSubTopic);
  }

  public static void selectFormat(String formatType) {
    ElementHelper.selectDropdownOption(PubSubLocators.format,
                                       CdfPluginPropertiesLocators.locateDropdownListItem(formatType));
  }

  public static void enterMaximumBatchCount(String maximumBatchcount) {
    ElementHelper.replaceElementValue(PubSubLocators.maximumBatchCount, maximumBatchcount);
  }

  public static void enterMaximumBatchSize(String maximumBatchSize) {
    ElementHelper.replaceElementValue(PubSubLocators.maximumBatchSize, maximumBatchSize);
  }

  public static void enterPublishDelayThreshold(String publishDelayThreshold) {
    ElementHelper.replaceElementValue(PubSubLocators.publishDelayThreshold, publishDelayThreshold);
  }

  public static void enterRetryTimeOut(String retryTimeOut) {
    ElementHelper.replaceElementValue(PubSubLocators.retryTimeout, retryTimeOut);
  }

  public static void enterErrorThreshold(String errorThreshold) {
    ElementHelper.replaceElementValue(PubSubLocators.errorThreshold, errorThreshold);
  }

  public static void close() {
    ElementHelper.clickOnElement(PubSubLocators.closeButton);
  }

  public static void enterEncryptionKeyName(String cmek) {
    ElementHelper.sendKeys(PubSubLocators.cmekKey, cmek);
  }

  public static void enterSubscription(String subscription) {
    ElementHelper.sendKeys(PubSubLocators.subscription, subscription);
  }

  public static void enterNumberOfReaders(String numberOfReaders) {
    ElementHelper.replaceElementValue(PubSubLocators.numberOfReaders, numberOfReaders);
  }

  public static void selectDataType() {
    Select select = new Select(PubSubLocators.messageDataType);
    select.selectByIndex(9);
  }
}
