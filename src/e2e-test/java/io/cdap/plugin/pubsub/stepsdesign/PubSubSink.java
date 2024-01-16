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

package io.cdap.plugin.pubsub.stepsdesign;

import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.pubsub.actions.PubSubActions;
import io.cdap.plugin.pubsub.locators.PubSubLocators;
import io.cdap.plugin.utils.E2EHelper;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cdap.plugin.utils.PubSubClient;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.openqa.selenium.support.ui.Select;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * PubSub Sink Plugin related step design.
 */

public class PubSubSink implements E2EHelper {

  @When("Sink is PubSub")
  public void sinkIsPubSub() {
    CdfStudioActions.expandPluginGroupIfNotAlreadyExpanded("Sink");
    selectSinkPlugin("GooglePublisher");
  }

  @Then("Enter PubSub sink property MaximumBatchCount {string}")
  public void enterPubSubSinkPropertyMaximumBatchCount(String pubSubMaximumBatchCount) {
    PubSubActions.enterMaximumBatchCount(PluginPropertyUtils.pluginProp(pubSubMaximumBatchCount));
  }

  @Then("Enter PubSub sink property MaximumBatchSize {string}")
  public void enterPubSubSinkPropertyMaximumBatchSize(String pubSubMaximumBatchSize) {
    PubSubActions.enterMaximumBatchSize(PluginPropertyUtils.pluginProp(pubSubMaximumBatchSize));
  }

  @Then("Enter PubSub sink property PublishDelayThreshold {string}")
  public void enterPubSubSinkPropertyPublishDelayThreshold(String pubSubPublishDelayThreshold) {
    PubSubActions.enterPublishDelayThreshold(PluginPropertyUtils.pluginProp(pubSubPublishDelayThreshold));
  }

  @Then("Enter PubSub sink property RetryTimeOut {string}")
  public void enterPubSubSinkPropertyRetryTimeOut(String pubSubRetryTimeOut) {
    PubSubActions.enterRetryTimeOut(PluginPropertyUtils.pluginProp(pubSubRetryTimeOut));
  }

  @Then("Enter PubSub sink property ErrorThreshold {string}")
  public void enterPubSubSinkPropertyErrorThreshold(String pubSubErrorThreshold) {
    PubSubActions.enterErrorThreshold(PluginPropertyUtils.pluginProp(pubSubErrorThreshold));
  }

  @Then("Open the PubSub sink properties")
  public void openThePubSubSinkProperties() {
    openSinkPluginProperties("GooglePublisher");
  }

  @Then("Enter PubSub sink property topic name")
  public void enterPubSubSinkPropertyTopicName() {
    PubSubActions.enterPubSubTopic(TestSetupHooks.pubSubTargetTopic);
  }

  @Then("Enter the PubSub sink properties with blank property {string}")
  public void enterThePubSubSinkPropertiesWithBlankProperty(String property) {
    if (property.equalsIgnoreCase("referenceName")) {
      PubSubActions.enterPubSubTopic("dummyTopic");
    } else if (property.equalsIgnoreCase("topic")) {
      PubSubActions.enterPubSubReferenceName();
    } else {
      Assert.fail("Invalid PubSub Mandatory Field : " + property);
    }
  }

  @Then("Enter the PubSub sink advanced properties with incorrect property {string}")
  public void enterThePubSubSinkAdvancedPropertiesWithIncorrectProperty(String property) {
    if (property.equalsIgnoreCase("messageCountBatchSize")) {
      ElementHelper.replaceElementValue(PubSubLocators.maximumBatchCount,
                                        PluginPropertyUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("requestThresholdKB")) {
      ElementHelper.replaceElementValue(PubSubLocators.maximumBatchSize,
                                        PluginPropertyUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("publishDelayThresholdMillis")) {
      ElementHelper.replaceElementValue(PubSubLocators.publishDelayThreshold,
                                        PluginPropertyUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("retryTimeoutSeconds")) {
      ElementHelper.replaceElementValue(PubSubLocators.retryTimeout,
                                        PluginPropertyUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("errorThreshold")) {
      ElementHelper.replaceElementValue(PubSubLocators.errorThreshold,
                                        PluginPropertyUtils.pluginProp("pubSubStringValue"));
    } else {
      Assert.fail("Invalid PubSub advanced property : " + property);
    }
  }

  @Then("Validate the error message for invalid PubSub sink advanced property {string}")
  public void validateTheErrorMessageForInvalidPubSubSinkAdvancedProperty(String property) {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = PluginPropertyUtils
      .errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_INVALID_ADVANCED_FIELDS).replaceAll("PROPERTY", property);
    String actualErrorMessage = PluginPropertyUtils.findPropertyErrorElement(property).getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = PluginPropertyUtils.getErrorColor(PluginPropertyUtils.findPropertyErrorElement(property));
    String expectedColor = ConstantsUtil.ERROR_MSG_COLOR;
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Enter the PubSub sink advanced properties with invalid number for property {string}")
  public void enterThePubSubSinkAdvancedPropertiesWithInvalidNumberForProperty(String property) {
    if (property.equalsIgnoreCase("messageCountBatchSize")) {
      ElementHelper.replaceElementValue(PubSubLocators.maximumBatchCount,
                                        PluginPropertyUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("requestThresholdKB")) {
      ElementHelper.replaceElementValue(PubSubLocators.maximumBatchSize,
                                        PluginPropertyUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("publishDelayThresholdMillis")) {
      ElementHelper.replaceElementValue(PubSubLocators.publishDelayThreshold,
                                        PluginPropertyUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("retryTimeoutSeconds")) {
      ElementHelper.replaceElementValue(PubSubLocators.retryTimeout,
                                        PluginPropertyUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("errorThreshold")) {
      ElementHelper.replaceElementValue(PubSubLocators.errorThreshold,
                                        PluginPropertyUtils.pluginProp("pubSubNegativeValue"));
    } else {
      Assert.fail("Invalid PubSub sink advanced property : " + property);
    }
  }

  @Then("Validate the number format error message for PubSub sink property {string}")
  public void validateTheNumberFormatErrorMessageForPubSubSinkProperty(String property) {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = StringUtils.EMPTY;
    if (property.equalsIgnoreCase("messageCountBatchSize")) {
      expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_MAX_BATCH_COUNT);
    } else if (property.equalsIgnoreCase("requestThresholdKB")) {
      expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_MAX_BATCH_SIZE);
    } else if (property.equalsIgnoreCase("publishDelayThresholdMillis")) {
      expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_DELAY_THRESHOLD);
    } else if (property.equalsIgnoreCase("retryTimeoutSeconds")) {
      expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_RETRY_TIMEOUT);
    } else if (property.equalsIgnoreCase("errorThreshold")) {
      expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_ERROR_THRESHOLD);
    } else {
      Assert.fail("Invalid PubSub sink Advanced Property " + property);
    }
    String actualErrorMessage = PluginPropertyUtils.findPropertyErrorElement(property).getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = PluginPropertyUtils.getErrorColor(PluginPropertyUtils.findPropertyErrorElement(property));
    String expectedColor = ConstantsUtil.ERROR_MSG_COLOR;
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Enter PubSub sink property encryption key name {string} if cmek is enabled")
  public void enterPubSubPropertyEncryptionKeyNameIfCmekIsEnabled(String cmek) {
    String cmekPubSub = PluginPropertyUtils.pluginProp(cmek);
    if (cmekPubSub != null) {
      PubSubActions.enterEncryptionKeyName(cmekPubSub);
      BeforeActions.scenario.write("Entered encryption key name - " + cmekPubSub);
    } else {
      BeforeActions.scenario.write("CMEK not enabled");
    }
  }

  @Then("Validate the cmek key {string} of target PubSub topic if cmek is enabled")
  public void validateTheCmekKeyOfTargetPubSubTopicIfCmekIsEnabled(String cmek) throws IOException {
    String cmekPubSub = PluginPropertyUtils.pluginProp(cmek);
    if (cmekPubSub != null) {
      Assert.assertEquals("Cmek key of target PubSub topic  should be equal to cmek key provided in config file"
        , PubSubClient.getTopicCmekKey(TestSetupHooks.pubSubTargetTopic), cmekPubSub);
    } else {
      BeforeActions.scenario.write("CMEK not enabled");
    }
  }

  @Then("Enter the PubSub sink mandatory properties")
  public void enterThePubSubSinkMandatoryProperties() {
    PubSubActions.enterPubSubReferenceName();
    PubSubActions.enterProjectID(PluginPropertyUtils.pluginProp("projectId"));
    PubSubActions.enterPubSubTopic(TestSetupHooks.pubSubTargetTopic);
  }

  @Then("Enter PubSub sink cmek property {string} as macro argument {string} if cmek is enabled")
  public void enterPubSubSinkCmekPropertyAsMacroArgumentIfCmekIsEnabled(String pluginProperty, String macroArgument) {
    String cmekPubSub = PluginPropertyUtils.pluginProp("cmekPubSub");
    if (cmekPubSub != null) {
      enterPropertyAsMacroArgument(pluginProperty, macroArgument);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Enter runtime argument value {string} for PubSub Sink cmek property key {string} " +
    "if PubSub Sink cmek is enabled")
  public void enterRuntimeArgumentValueForPubSubSinkCmekPropertyKeyIfPubSubSinkCmekIsEnabled
    (String value, String runtimeArgumentKey) {
    String cmekPubSub = PluginPropertyUtils.pluginProp(value);
    if (cmekPubSub != null) {
      ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey), cmekPubSub);
      BeforeActions.scenario.write("PubSubSink encryption key name - " + cmekPubSub);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Enter runtime argument value for PubSub sink property topic key {string}")
  public void enterRuntimeArgumentValueForPubSubSinkPropertyTopicKey(String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey), TestSetupHooks.pubSubTargetTopic);
  }

  @Then("Click on preview data for PubSub sink")
  public void clickOnPreviewDataForPubSubSink() {
    openSinkPluginPreviewData("GooglePublisher");
  }

  @Then("Subscribe to the messages")
  public void subscribeToTheMessages() throws InterruptedException {
    TimeUnit time = TimeUnit.SECONDS;
    time.sleep(60);
    PubSubClient.subscribeAsyncExample(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID),
                                       TestSetupHooks.pubSubSourceSubscription);
  }

  @And("Click on batch time and select format")
  public void clickOnBatchTimeAndSelectFormat() {
    Select select = new Select(PubSubLocators.batchTime);
    select.selectByIndex(0);
    Select selectformat = new Select(PubSubLocators.timeSelect);
    selectformat.selectByIndex(1);
    ElementHelper.clickOnElement(PubSubLocators.saveButton);
  }

  @And("Click on configure button")
  public void clickOnConfigureButton() {
    ElementHelper.clickOnElement(PubSubLocators.configButton);
  }

  @And("Click on pipeline config")
  public void clickOnPipelineConfig() {
    ElementHelper.clickOnElement(PubSubLocators.pipelineConfig);
  }
}
