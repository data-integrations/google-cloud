/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.datastore.stepsdesign;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;

import io.cdap.plugin.datastore.actions.DataStoreActions;
import io.cdap.plugin.utils.DataStoreClient;
import io.cucumber.java.en.Then;
import org.junit.Assert;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * DataStore Plugin related step design.
 */
public class StepDesign {
  @Then("Enter kind for datastore plugin")
  public void enterKindForDatastorePlugin() {
  DataStoreActions.enterKind(TestSetupHooks.kindName);

  }

  @Then("Enter Ancestor for the datastore plugin")
  public void enterAncestorForTheDatastorePlugin() {
    DataStoreActions.enterAncestor();
  }

  @Then("Validate The Data From Datastore To Datastore With Actual And Expected File for: {string}")
  public void validateTheDataFromDatastoreToDatastoreWithActualAndExpectedFileFor(String expectedFile)
    throws URISyntaxException {
    boolean recordsMatched = DataStoreClient.validateActualDataToExpectedData(
      PluginPropertyUtils.pluginProp("kindName"),
      PluginPropertyUtils.pluginProp(expectedFile));
    Assert.assertTrue("Value of records in actual and expected file is equal", recordsMatched);
  }
}

