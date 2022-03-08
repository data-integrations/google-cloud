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
package io.cdap.plugin.gcs.stepsdesign;

import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.utils.E2EHelper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;

/**
 * GCS Sink Plugin related step design.
 */
public class GCSSink implements E2EHelper {

  @When("Sink is GCS")
  public void sinkIsGCS() {
    CdfStudioActions.clickSink();
    selectSinkPlugin("GCS");
  }

  @Then("Open GCS sink properties")
  public void openGCSSinkProperties() {
    openSinkPluginProperties("GCS");
  }

  @Then("Enter GCS sink property path")
  public void enterGCSSinkPropertyPath() {
    CdfGcsActions.getGcsBucket("gs://" + TestSetupHooks.gcsTargetBucketName);
  }

  @Then("Enter the GCS sink mandatory properties")
  public void enterTheGCSSinkMandatoryProperties() throws IOException, InterruptedException {
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.getGcsBucket(TestSetupHooks.gcsTargetBucketName);
    CdfGcsActions.selectFormat("json");
  }

  @Then("Enter GCS property encryption key name {string} if cmek is enabled")
  public void enterGCSPropertyEncryptionKeyNameIfCmekIsEnabled(String cmek) {
    String cmekGCS = PluginPropertyUtils.pluginProp(cmek);
    if (cmekGCS != null) {
      CdfGcsActions.enterEncryptionKeyName(cmekGCS);
      BeforeActions.scenario.write("Entered encryption key name - " + cmekGCS);
    }
  }

  @Then("Click on preview data for GCS sink")
  public void clickOnPreviewDataForGCSSink() {
    openSinkPluginPreviewData("GCS");
  }

  @Then("Validate the cmek key {string} of target GCS bucket if cmek is enabled")
  public void validateTheCmekKeyOfTargetGCSBucketIfCmekIsEnabled(String cmek) throws IOException {
    String cmekGCS = PluginPropertyUtils.pluginProp(cmek);
    if (cmekGCS != null) {
      Assert.assertEquals("Cmek key of target GCS bucket should be equal to cmek key provided in config file"
        , StorageClient.getBucketCmekKey(TestSetupHooks.gcsTargetBucketName), cmekGCS);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Enter GCS sink property {string} as macro argument {string}")
  public void enterGCSSinkPropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    enterPropertyAsMacroArgument(pluginProperty, macroArgument);
  }

  @Then("Enter GCS sink cmek property {string} as macro argument {string} if cmek is enabled")
  public void enterGCSSinkCmekPropertyAsMacroArgumentIfCmekIsEnabled(String pluginProperty, String macroArgument) {
    String cmekGCS = PluginPropertyUtils.pluginProp("cmekGCS");
    if (cmekGCS != null) {
      enterPropertyAsMacroArgument(pluginProperty, macroArgument);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Enter runtime argument value for GCS sink property path key {string}")
  public void enterRuntimeArgumentValueForGCSSinkPropertyPathKey(String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey),
                           "gs://" + TestSetupHooks.gcsTargetBucketName);
  }

  @Then("Enter runtime argument value {string} for GCS cmek property key {string} if GCS cmek is enabled")
  public void enterRuntimeArgumentValueForGCSCmekPropertyKeyIfGCSCmekIsEnabled(String value,
                                                                               String runtimeArgumentKey) {
    String cmekGCS = PluginPropertyUtils.pluginProp(value);
    if (cmekGCS != null) {
      ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey), cmekGCS);
      BeforeActions.scenario.write("GCS encryption key name - " + cmekGCS);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }
}
