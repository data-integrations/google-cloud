/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.gcsmove.stepsdesign;

import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.gcsmove.actions.GCSMoveActions;
import io.cdap.plugin.gcsmove.locators.GCSDoneFileMarkerLocators;
import io.cdap.plugin.utils.E2EHelper;
import io.cucumber.java.en.Then;
import org.openqa.selenium.By;

/**
 * GCSDoneFileMarker related steps definitions.
 */
public class GCSDoneFileMarker implements E2EHelper {

  static {
    SeleniumHelper.getPropertiesLocators(GCSDoneFileMarkerLocators.class);
  }

  @Then("Open pipeline alerts from pipeline configure")
  public void openPipelineAlertsFromPipelineConfigure() {
    ElementHelper.clickOnElement(GCSDoneFileMarkerLocators.pipelineConfigureButton);
    ElementHelper.clickOnElement(GCSDoneFileMarkerLocators.pipelineAlertsItem);
    ElementHelper.clickOnElement(GCSDoneFileMarkerLocators.pipelineAlertsCreate);
  }

  @Then("Save Pipeline alerts and close")
  public void saveAndClosePipelineAlerts() {
    WaitHelper.waitForElementToBePresent(By.xpath(GCSDoneFileMarkerLocators.XPATH_PIPELINE_ALERTS_APPLY));
    ElementHelper.clickOnElement(GCSDoneFileMarkerLocators.pipelineAlertsApply);
  }

  @Then("Open GCSDoneFileMarker to configure")
  public void openCreateMarkerFileToConfigure() {
    ElementHelper.clickOnElement(GCSDoneFileMarkerLocators.gcsDoneFileMarkerButton);
  }

  @Then("Select GCSDoneFileMarker property Run condition as {string}")
  public void selectRunConditionAs(String value) {
    ElementHelper.clickOnElement(GCSDoneFileMarkerLocators.gcsFileMarkerRunConditionSelect);
    ElementHelper.clickOnElement(SeleniumDriver.getDriver().findElement
      (By.xpath("//*[@data-cy='option-" + value + "']")));
  }

  @Then("Confirm GCSDoneFileMarker configuration")
  public void confirmGCSDoneFileMarkerConfiguration() {
    ElementHelper.clickOnElement(GCSDoneFileMarkerLocators.gcsFileMarkerNextBtn);
    WaitHelper.waitForElementToBePresent(By.xpath(GCSDoneFileMarkerLocators.XPATH_GCS_FILE_MARKER_CONFIRM));
    ElementHelper.clickOnElement(GCSDoneFileMarkerLocators.gcsFileMarkerConfirmBtn);
  }

  @Then("Enter GCSDoneFileMarker destination path with {string}")
  public void enterGCSDoneFileMarkerDestinationPathWith(String path) {
    GCSMoveActions.enterFileMarkerPath("gs://" + TestSetupHooks.gcsTargetBucketName
                                         + "/" + PluginPropertyUtils.pluginProp(path));
  }
}
