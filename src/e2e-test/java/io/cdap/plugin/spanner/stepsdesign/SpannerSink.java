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
package io.cdap.plugin.spanner.stepsdesign;

import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.spanner.actions.SpannerActions;
import io.cdap.plugin.utils.E2EHelper;
import io.cdap.plugin.utils.SpannerClient;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import stepsdesign.BeforeActions;

/**
 * Spanner sink plugin related test step definitions.
 */
public class SpannerSink implements E2EHelper {

  @When("Sink is Spanner")
  public void sinkIsSpanner() {
    CdfStudioActions.clickSink();
    selectSinkPlugin("Spanner");
  }

  @Then("Open Spanner sink properties")
  public void openSpannerSinkProperties() {
    openSinkPluginProperties("Spanner");
  }

  @Then("Click on preview data for Spanner sink")
  public void clickOnPreviewDataForSpannerSink() {
    openSinkPluginPreviewData("Spanner");
  }

  @Then("Enter Spanner sink property DatabaseName")
  public void enterSpannerSinkPropertyDatabaseName() {
    SpannerActions.enterDatabaseName(TestSetupHooks.spannerTargetDatabase);
  }

  @Then("Enter Spanner sink property TableName")
  public void enterSpannerSinkPropertyTableName() {
    SpannerActions.enterTableName(TestSetupHooks.spannerTargetTable);
  }

  @Then("Enter Spanner sink property primary key {string}")
  public void enterSpannerSinkPropertyPrimaryKey(String primaryKey) {
    SpannerActions.enterPrimaryKey(PluginPropertyUtils.pluginProp(primaryKey));
  }

  @Then("Enter runtime argument value for Spanner Sink Table Name key {string}")
  public void enterRuntimeArgumentValueForSpannerSinkTableNameKey(String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey), TestSetupHooks.spannerTargetTable);
  }

  @Then("Enter Spanner cmek property {string} as macro argument {string} if cmek is enabled")
  public void enterSpannerCmekPropertyAsMacroArgumentIfCmekIsEnabled(String pluginProperty, String macroArgument) {
    String cmekSpanner = PluginPropertyUtils.pluginProp("cmekSpanner");
    if (cmekSpanner != null) {
      enterPropertyAsMacroArgument(pluginProperty, macroArgument);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Enter Spanner sink property encryption key name {string} if cmek is enabled")
  public void enterSpannerSinkPropertyEncryptionKeyNameStringIfCmekIsEnabled(String cmek) {
    String cmekSpanner = PluginPropertyUtils.pluginProp(cmek);
    if (cmekSpanner != null) {
      SpannerActions.enterEncryptionKeyName(cmekSpanner);
      BeforeActions.scenario.write("Entered encryption key name - " + cmekSpanner);
    } else {
      BeforeActions.scenario.write("CMEK not enabled");
    }
  }

  @Then("Enter runtime argument value {string} for Spanner cmek property key {string} if Spanner cmek is enabled")
  public void enterRuntimeArgumentValueForSpannerCmekPropertyKeyIfSpannerCmekIsEnabled
    (String value, String runtimeArgumentKey) {
    String cmekSpanner = PluginPropertyUtils.pluginProp(value);
    if (cmekSpanner != null) {
      ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey), cmekSpanner);
      BeforeActions.scenario.write("Spanner encryption key name - " + cmekSpanner);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Validate the cmek key {string} of target Spanner database if cmek is enabled")
  public void validateTheCmekKeyOfTargetSpannerDatabaseIfCmekIsEnabled(String cmek) {
    String cmekSpanner = PluginPropertyUtils.pluginProp(cmek);
    if (cmekSpanner != null) {
      Assert.assertEquals("Cmek key of target Spanner database should be equal to cmek key provided in config file"
        , SpannerClient.databaseCmekKey(TestSetupHooks.spannerInstance
          , TestSetupHooks.spannerTargetDatabase), cmekSpanner);
    } else {
      BeforeActions.scenario.write("CMEK not enabled");
    }
  }
}
