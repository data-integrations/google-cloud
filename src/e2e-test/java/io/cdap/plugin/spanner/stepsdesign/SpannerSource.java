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

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.spanner.actions.SpannerActions;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

/**
 * Spanner source plugin related test step definitions.
 */
public class SpannerSource implements CdfHelper {

  @When("Source is Spanner")
  public void sourceIsSpanner() {
    selectSourcePlugin("Spanner");
  }

  @Then("Open Spanner source properties")
  public void openSpannerSourceProperties() {
    openSourcePluginProperties("Spanner");
  }

  @Then("Enter Spanner source property DatabaseName")
  public void enterSpannerSourcePropertyDatabaseName() {
    SpannerActions.enterDatabaseName(TestSetupHooks.spannerDatabase);
  }

  @Then("Enter Spanner source property TableName")
  public void enterSpannerSourcePropertyTableName() {
    SpannerActions.enterTableName(TestSetupHooks.spannerSourceTable);
  }

  @Then("Enter the Spanner source property Import Query {string}")
  public void enterTheSpannerSourcePropertyImportQuery(String query) {
    SpannerActions.enterImportQuery(PluginPropertyUtils.pluginProp(query));
  }
}
