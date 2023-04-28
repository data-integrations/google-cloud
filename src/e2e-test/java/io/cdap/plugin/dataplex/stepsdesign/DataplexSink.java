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

import io.cdap.e2e.pages.actions.CdfPluginPropertiesActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.utils.DataplexHelper;
import io.cdap.plugin.utils.E2EHelper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.io.IOException;


/**
 * Dataplex Sink related stepDesigns.
 */
public class DataplexSink implements E2EHelper {

  @When("Sink is Dataplex")
  public void sinkIsDataplex() {
    CdfStudioActions.clickSink();
    selectSinkPlugin("Dataplex");
  }

  @Then("Open Dataplex sink properties")
  public void openDataplexSinkProperties() {
    openSinkPluginProperties("Dataplex");
  }

  @Then("Close the Dataplex properties")
  public void closeTheBigQueryProperties() {
    CdfPluginPropertiesActions.clickCloseButton();
  }

  @Then("Enter the Dataplex sink mandatory properties")
  public void enterTheDataplexSinkMandatoryProperties() throws IOException {
    DataplexHelper.enterDataplexProperty("asset", PluginPropertyUtils.pluginProp("dataplexDefaultAsset"));
    DataplexHelper.setAssetType("STORAGE_BUCKET");
    DataplexHelper.enterDataplexProperty("table", TestSetupHooks.gcsTargetBucketName);
  }

  @Then("Enable Metadata Update")
  public void enableMetadataUpdate() {
    DataplexHelper.toggleMetadataUpdate();
  }

  @Then("Remove {string} column from output schema")
  public void removeColumnFromOutputSchema(String fieldName) throws InterruptedException {
    DataplexHelper.deleteSchemaField(fieldName);
  }
}
