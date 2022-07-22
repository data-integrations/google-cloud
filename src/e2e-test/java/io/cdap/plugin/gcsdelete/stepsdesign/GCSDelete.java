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
package io.cdap.plugin.gcsdelete.stepsdesign;

import com.google.cloud.storage.Blob;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.gcsdelete.actions.GCSDeleteActions;
import io.cdap.plugin.gcsdelete.locators.GCSDeleteLocators;
import io.cdap.plugin.utils.E2EHelper;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;

/**
 * GCSDelete Action related stepDesigns.
 */

public class GCSDelete implements E2EHelper {

  @Then("Enter the GCS Delete property projectId {string}")
  public void enterTheGCSDeletePropertyProjectId(String projectId) {
    GCSDeleteActions.enterProjectId(PluginPropertyUtils.pluginProp(projectId));
  }

  @Then("Enter the GCS Delete property objects to delete as bucketName")
  public void enterTheGCSDeletePropertyObjectsToDeleteAsBucketName() {
    GCSDeleteActions.enterObjectsToDelete(0, "gs://" + TestSetupHooks.gcsSourceBucketName);
  }

  @Then("Enter the GCS Delete property objects to delete as path {string}")
  public void enterTheGCSDeletePropertyObjectsToDeleteAsPath(String path) {
    GCSDeleteActions.enterObjectsToDelete(0, "gs://" + TestSetupHooks.gcsSourceBucketName + "/"
      + PluginPropertyUtils.pluginProp(path));
  }

  @Then("Enter the GCS Delete property objects to delete as list of objects {string}")
  public void enterTheGCSDeletePropertyObjectsToDeleteAsListOfObjects(String commaSeparatedObjectsList) {
    String[] objectsToDelete = PluginPropertyUtils.pluginProp(commaSeparatedObjectsList).split(",");
    for (int index = 0; index < objectsToDelete.length; index++) {
      GCSDeleteActions.enterObjectsToDelete(index, "gs://" + TestSetupHooks.gcsSourceBucketName + "/"
        + objectsToDelete[index]);
      GCSDeleteActions.clickObjectsToDeleteAddRowButton(index);
    }
  }

  @Then("Close the GCS Delete properties")
  public void closeTheGCSDeleteProperties() {
    CdfStudioActions.clickCloseButton();
  }

  @Then("Verify invalid bucket name error message is displayed for GCS Delete objects to delete path {string}")
  public void verifyInvalidBucketNameErrorMessageIsDisplayedForGCSDeleteObjectsToDeletePath(String path) {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_GCS_INVALID_BUCKET_NAME)
      .replace("BUCKET_NAME", "/" + PluginPropertyUtils.pluginProp(path));
    String actualErrorMessage = GCSDeleteLocators.objectsToDeletePropertyError(0).getText();
    Assert.assertEquals("Invalid bucket name error message should be displayed",
                        expectedErrorMessage, actualErrorMessage);
    String actualColor = PluginPropertyUtils.getErrorColor(GCSDeleteLocators.objectsToDeletePropertyError(0));
    String expectedColor = ConstantsUtil.ERROR_MSG_COLOR;
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Verify all the objects in the GCS bucket deleted successfully by GCS Delete action plugin")
  public void verifyAllTheObjectsInTheGCSBucketDeletedSuccessfullyByGCSDeleteActionPlugin() throws IOException {
    for (Blob blob : StorageClient.listObjects(TestSetupHooks.gcsSourceBucketName).iterateAll()) {
      Assert.fail("All objects in the GCS Bucket " + TestSetupHooks.gcsSourceBucketName +
                    " not deleted by GCS Delete plugin.");
    }
    BeforeActions.scenario.write("All objects in the GCS Bucket  " + TestSetupHooks.gcsSourceBucketName
                                   + " deleted successfully by GCS Delete plugin.");
  }

  @Then("Verify objects in the GCS path {string} deleted successfully by GCS Delete action plugin")
  public void verifyObjectsInTheGCSPathDeletedSuccessfullyByGCSDeleteActionPlugin(String path) throws IOException {
    String gcsPath = PluginPropertyUtils.pluginProp(path);
    Blob gcsObject = StorageClient.getObjectMetadata(TestSetupHooks.gcsSourceBucketName, gcsPath);
    if (gcsObject != null) {
      Assert.fail("GCS object at path " + TestSetupHooks.gcsSourceBucketName + "/" + gcsPath
                    + " is not deleted by GCS Delete plugin.");
    }
    BeforeActions.scenario.write("Objects in the path  " + TestSetupHooks.gcsSourceBucketName + "/" + gcsPath
                                   + " deleted successfully by GCS Delete plugin.");
  }

  @Then("Verify multiple objects {string} deleted successfully by GCS Delete action plugin")
  public void verifyMultipleObjectsDeletedSuccessfullyByGCSDeleteActionPlugin(String commaSeparatedObjectsList)
    throws IOException {
    String[] objectsToDelete = PluginPropertyUtils.pluginProp(commaSeparatedObjectsList).split(",");
    for (int index = 0; index < objectsToDelete.length; index++) {
      Blob gcsObject = StorageClient.getObjectMetadata(TestSetupHooks.gcsSourceBucketName, objectsToDelete[index]);
      if (gcsObject != null) {
        Assert.fail("GCS object " + TestSetupHooks.gcsSourceBucketName + "/" + objectsToDelete[index]
                      + " is not deleted by GCS Delete plugin.");
      }
    }
    BeforeActions.scenario.write("Multiple objects deleted successfully by GCS Delete plugin.");
  }

  @Then("Verify objects {string} still exist after GCS Delete action plugin")
  public void verifyObjectsExistByGCSDeleteActionPlugin(String commaSeparatedObjectsList)
    throws IOException {
    String[] objectsToKeep = PluginPropertyUtils.pluginProp(commaSeparatedObjectsList).split(",");
    for (int index = 0; index < objectsToKeep.length; index++) {
      Blob gcsObject = StorageClient.getObjectMetadata(TestSetupHooks.gcsSourceBucketName, objectsToKeep[index]);
      if (gcsObject == null) {
        Assert.fail("GCS object " + TestSetupHooks.gcsSourceBucketName + "/" + objectsToKeep[index]
                      + " is deleted by GCS Delete plugin.");
      }
    }
    BeforeActions.scenario.write("Multiple objects deleted successfully by GCS Delete plugin.");
  }

  @Then("Enter the GCS Delete property {string} as macro argument {string}")
  public void enterTheGCSDeletePropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    enterPropertyAsMacroArgument(pluginProperty, macroArgument);
  }

  @Then("Enter runtime argument value source bucket name for key {string}")
  public void enterRuntimeArgumentValueSourceBucketNameForKey(String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey),
                           "gs://" + TestSetupHooks.gcsSourceBucketName);
  }

  @Then("Enter runtime argument value {string} as comma separated objects for key {string}")
  public void enterRuntimeArgumentValueAsCommaSeparatedObjectsForKey(
    String value, String runtimeArgumentKey) {
    String[] objectToDelete = PluginPropertyUtils.pluginProp(value).split(",");
    for (String s : objectToDelete) {
      ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey),
                             "gs://" + TestSetupHooks.gcsSourceBucketName + "/" + s + ",");
    }
  }
}
