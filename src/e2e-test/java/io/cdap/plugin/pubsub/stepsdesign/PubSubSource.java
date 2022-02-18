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

import io.cdap.e2e.utils.CdfHelper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

/**
 * PubSub Source Plugin related step design.
 */

public class PubSubSource implements CdfHelper {

  @When("Source is PubSub")
  public void sourceIsPubSub() {
    selectSourcePlugin("GooglePublisher");
  }

  @Then("Open the PubSub source properties")
  public void openThePubSubSourceProperties() {
    openSourcePluginProperties("GooglePublisher");
  }
}
