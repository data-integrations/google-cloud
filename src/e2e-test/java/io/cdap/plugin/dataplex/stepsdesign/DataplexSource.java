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
package io.cdap.plugin.dataplex.stepsdesign;

import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.utils.DataplexHelper;
import io.cdap.plugin.utils.E2EHelper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.io.IOException;

/**
 * Dataplex Source related stepDesigns.
 */
public class DataplexSource implements E2EHelper {

  @When("Source is Dataplex")
  public void sourceIsDataplex() {
    selectSourcePlugin("Dataplex");
  }

  @Then("Open Dataplex source properties")
  public void openDataplexSourceProperties() {
    openSourcePluginProperties("Dataplex");
  }

  @Then("Enter the Dataplex source mandatory properties")
  public void enterTheDataplexSourceMandatoryProperties() throws IOException {
    DataplexHelper.enterDataplexProperty(
      "entity", TestSetupHooks.gcsTargetBucketName.replaceAll("[^a-zA-Z0-9_]", "_"));
  }
}
