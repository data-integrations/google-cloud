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

import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.utils.DataplexHelper;
import io.cdap.plugin.utils.E2EHelper;
import io.cucumber.java.en.Then;

import java.io.IOException;

/**
 * Dataplex related common stepDesigns.
 */
public class DataplexBase implements E2EHelper {
  @Then("Enter the Dataplex mandatory properties")
  public void enterTheDataplexMandatoryProperties() throws IOException {
    DataplexHelper.enterReferenceName();
    DataplexHelper.enterProjectId();
    DataplexHelper.enterDataplexProperty(
      "location", PluginPropertyUtils.pluginProp("dataplexDefaultLocation"));
    DataplexHelper.enterDataplexProperty("lake", PluginPropertyUtils.pluginProp("dataplexDefaultLake"));
    DataplexHelper.enterDataplexProperty("zone", PluginPropertyUtils.pluginProp("dataplexDefaultZone"));
  }
}
