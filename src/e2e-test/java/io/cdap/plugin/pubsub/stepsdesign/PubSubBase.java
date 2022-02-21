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
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.pubsub.actions.PubSubActions;
import io.cucumber.java.en.Then;

/**
 * PubSub Plugin related common step design.
 */

public class PubSubBase implements CdfHelper {

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

  @Then("Enter PubSub topic name {string}")
  public void enterPubSubTopicName(String topicName) {
    PubSubActions.enterPubSubTopic(topicName);
  }

  @Then("Close the PubSub properties")
  public void closeThePubSubProperties() {
    PubSubActions.close();
  }
}
