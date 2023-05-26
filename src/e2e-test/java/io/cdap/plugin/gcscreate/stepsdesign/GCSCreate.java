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
package io.cdap.plugin.gcscreate.stepsdesign;

import com.google.cloud.storage.Blob;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.gcscreate.actions.GCSCreateActions;
import io.cdap.plugin.gcsmove.actions.GCSMoveActions;
import io.cdap.plugin.utils.E2EHelper;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;

/**
 * GCSCreate Action related stepDesigns.
 */
public class GCSCreate implements E2EHelper {

  @Then("Enter the GCS Create property projectId {string}")
  public void enterTheGCSCreatePropertyProjectId(String projectId) {
    GCSCreateActions.enterProjectId(PluginPropertyUtils.pluginProp(projectId));
  }

  @Then("Enter the GCS Create property objects to create as path {string}")
  public void enterTheGCSCreatePropertyObjectsToCreate(String path) {
    GCSCreateActions.enterObjectsToCreate(0, "gs://" + TestSetupHooks.gcsSourceBucketName
                                     + "/" + PluginPropertyUtils.pluginProp(path));
  }

  @Then("Close the GCS Create properties")
  public void closeTheGCSCreateProperties() {
    CdfStudioActions.clickCloseButton();
  }

  @Then("Select GCS Create property fail if objects exists as {string}")
  public void selectGCSCreatePropertyFailIfObjectsExistsAs(String value) {
    GCSCreateActions.selectFailIfObjectsExists(value);
  }

  @Then("Verify that the object {string} created successfully")
  public void verifyThatTheObjectCreatedSuccessfully(String path) throws IOException {
    String sourceGCSBucket = TestSetupHooks.gcsSourceBucketName;
    String gcsObject = PluginPropertyUtils.pluginProp(path);

    boolean isObjectFoundInBucket = false;
    for (Blob blob : StorageClient.listObjects(sourceGCSBucket).iterateAll()) {
      if (blob.getName().contains(gcsObject)) {
        isObjectFoundInBucket = true;
        break;
      }
    }
    if (isObjectFoundInBucket) {
      BeforeActions.scenario.write("Object is created in source GCS Bucket " + sourceGCSBucket + " successfully");
    } else {
      Assert.fail("Object is not created in source bucket" + sourceGCSBucket);
    }
  }

  @Then("Enter GCSCreate property {string} as macro argument {string}")
  public void enterGCSCreatePropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    enterPropertyAsMacroArgument(pluginProperty, macroArgument);
  }

  @Then("Enter GCSCreate property encryption key name {string} if cmek is enabled")
  public void enterGCSCreatePropertyEncryptionKeyNameIfCmekIsEnabled(String cmek) {
    String cmekGCS = PluginPropertyUtils.pluginProp(cmek);
    if (cmekGCS != null) {
      GCSMoveActions.enterEncryptionKeyName(cmekGCS);
      BeforeActions.scenario.write("Entered encryption key name - " + cmekGCS);
    }
  }

  @Then("Enter GCSCreate cmek property {string} as macro argument {string} if cmek is enabled")
  public void enterGCSCreateCmekPropertyAsMacroArgumentIfCmekIsEnabled(String pluginProperty, String macroArgument) {
    String cmekGCS = PluginPropertyUtils.pluginProp("cmekGCS");
    if (cmekGCS != null) {
      enterPropertyAsMacroArgument(pluginProperty, macroArgument);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Enter runtime argument value {string} for GCSCreate property key {string}")
  public void enterRuntimeArgumentValueForGCSCreatePropertyKey(String value, String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey),
                           "gs://" + TestSetupHooks.gcsSourceBucketName + "/" + PluginPropertyUtils.pluginProp(value));
  }
}
