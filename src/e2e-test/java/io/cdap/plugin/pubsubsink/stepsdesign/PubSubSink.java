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

package io.cdap.plugin.pubsubsink.stepsdesign;

import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.pubsubsink.actions.PubSubActions;
import io.cdap.plugin.pubsubsink.locators.PubSubLocators;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cdap.plugin.utils.PubSubClient;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;

/**
 * PubSub Sink Plugin related step design.
 */

public class PubSubSink implements CdfHelper {

  @When("Sink is PubSub")
  public void sinkIsPubSub() {
    CdfStudioActions.clickSink();
    selectSinkPlugin("GooglePublisher");
  }

  @Then("Close the PubSub properties")
  public void closeThePubSubProperties() {
    PubSubActions.close();
  }

  @Then("Enter PubSub property projectId {string}")
  public void enterPubSubPropertyProjectId(String projectId) {
    PubSubActions.enterProjectID(PluginPropertyUtils.pluginProp(projectId));
  }

  @Then("Enter PubSub property reference name")
  public void enterPubSubPropertyReferenceName() {
    PubSubActions.enterPubSubReferenceName();
  }

  @Then("Select PubSub property format {string}")
  public void selectPubSubPropertyFormat(String format) {
    PubSubActions.selectFormat(format);
  }

  @Then("Enter PubSub property MaximumBatchCount {string}")
  public void enterPubSubPropertyMaximumBatchCount(String pubSubMaximumBatchCount) {
    PubSubActions.enterMaximumBatchCount(PluginPropertyUtils.pluginProp(pubSubMaximumBatchCount));
  }

  @Then("Enter PubSub property MaximumBatchSize {string}")
  public void enterPubSubPropertyMaximumBatchSize(String pubSubMaximumBatchSize) {
    PubSubActions.enterMaximumBatchSize(PluginPropertyUtils.pluginProp(pubSubMaximumBatchSize));
  }

  @Then("Enter PubSub property PublishDelayThreshold {string}")
  public void enterPubSubPropertyPublishDelayThreshold(String pubSubPublishDelayThreshold) {
    PubSubActions.enterPublishDelayThreshold(PluginPropertyUtils.pluginProp(pubSubPublishDelayThreshold));
  }

  @Then("Enter PubSub property RetryTimeOut {string}")
  public void enterPubSubPropertyRetryTimeOut(String pubSubRetryTimeOut) {
    PubSubActions.enterRetryTimeOut(PluginPropertyUtils.pluginProp(pubSubRetryTimeOut));
  }

  @Then("Enter PubSub property ErrorThreshold {string}")
  public void enterPubSubPropertyErrorThreshold(String pubSubErrorThreshold) {
    PubSubActions.enterErrorThreshold(PluginPropertyUtils.pluginProp(pubSubErrorThreshold));
  }

  @Then("Open the PubSub sink properties")
  public void openThePubSubSinkProperties() {
    openSinkPluginProperties("GooglePublisher");
  }

  @Then("Enter PubSub topic name")
  public void enterPubSubTopicName() {
    PubSubActions.enterPubSubTopic(TestSetupHooks.pubSubTargetTopic);
  }

  @Then("Enter PubSub topic name {string}")
  public void enterPubSubTopicName(String topicName) {
    PubSubActions.enterPubSubTopic(topicName);
  }

  @Then("Enter the PubSub property with blank property {string}")
  public void enterThePubSubPropertyWithBlankProperty(String property) {
    if (property.equalsIgnoreCase("referenceName")) {
      PubSubActions.enterPubSubTopic("dummyTopic");
    } else if (property.equalsIgnoreCase("topic")) {
      PubSubActions.enterPubSubReferenceName();
    } else {
      Assert.fail("Invalid PubSub Mandatory Field : " + property);
    }
  }

  @Then("Enter the PubSub advanced properties with incorrect property {string}")
  public void enterThePubSubAdvancedPropertiesWithIncorrectProperty(String property) {
    if (property.equalsIgnoreCase("messageCountBatchSize")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.maximumBatchCount,
                                         PluginPropertyUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("requestThresholdKB")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.maximumBatchSize,
                                         PluginPropertyUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("publishDelayThresholdMillis")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.publishDelayThreshold,
                                         PluginPropertyUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("retryTimeoutSeconds")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.retryTimeout,
                                         PluginPropertyUtils.pluginProp("pubSubStringValue"));
    } else if (property.equalsIgnoreCase("errorThreshold")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.errorThreshold,
                                         PluginPropertyUtils.pluginProp("pubSubStringValue"));
    } else {
      Assert.fail("Invalid PubSub advanced property : " + property);
    }
  }

  @Then("Validate the error message for invalid PubSub advanced property {string}")
  public void validateTheErrorMessageForInvalidPubSubAdvancedProperty(String property) {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = PluginPropertyUtils
      .errorProp(E2ETestConstants.ERROR_MSG_PUBSUB_INVALID_ADVANCED_FIELDS).replaceAll("PROPERTY", property);
    String actualErrorMessage = PluginPropertyUtils.findPropertyErrorElement(property).getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = PluginPropertyUtils.getErrorColor(PluginPropertyUtils.findPropertyErrorElement(property));
    String expectedColor = ConstantsUtil.ERROR_MSG_COLOR;
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Enter the PubSub advanced properties with invalid number for property {string}")
  public void enterThePubSubAdvancedPropertiesWithInvalidNumberForProperty(String property) {
    if (property.equalsIgnoreCase("messageCountBatchSize")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.maximumBatchCount,
                                         PluginPropertyUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("requestThresholdKB")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.maximumBatchSize,
                                         PluginPropertyUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("publishDelayThresholdMillis")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.publishDelayThreshold,
                                         PluginPropertyUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("retryTimeoutSeconds")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.retryTimeout,
                                         PluginPropertyUtils.pluginProp("pubSubNegativeValue"));
    } else if (property.equalsIgnoreCase("errorThreshold")) {
      SeleniumHelper.replaceElementValue(PubSubLocators.errorThreshold,
                                         PluginPropertyUtils.pluginProp("pubSubNegativeValue"));
    } else {
      Assert.fail("Invalid PubSub advanced property : " + property);
    }
  }

  @Then("Validate the number format error message for PubSub property {string}")
  public void validateTheNumberFormatErrorMessageForPubSubProperty(String property) {
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
      Assert.fail("Invalid PubSub Advanced Property " + property);
    }
    String actualErrorMessage = PluginPropertyUtils.findPropertyErrorElement(property).getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = PluginPropertyUtils.getErrorColor(PluginPropertyUtils.findPropertyErrorElement(property));
    String expectedColor = ConstantsUtil.ERROR_MSG_COLOR;
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Enter PubSub property encryption key name {string} if cmek is enabled")
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
}
