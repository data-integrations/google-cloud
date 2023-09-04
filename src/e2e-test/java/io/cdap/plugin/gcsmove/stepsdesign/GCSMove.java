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
package io.cdap.plugin.gcsmove.stepsdesign;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.gcsmove.actions.GCSMoveActions;
import io.cdap.plugin.utils.E2EHelper;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;
import stepsdesign.BeforeActions;

import java.io.IOException;

/**
 * GCSMove related steps definitions.
 */
public class GCSMove implements E2EHelper {

  @Then("Enter GCSMove property projectId {string}")
  public void enterGCSMovePropertyProjectId(String projectId) throws IOException {
    GCSMoveActions.enterProjectId(PluginPropertyUtils.pluginProp(projectId));
  }

  @Then("Enter GCSMove property source path {string}")
  public void enterGCSMovePropertySourcePath(String path) {
    GCSMoveActions.enterSourcePath("gs://" + TestSetupHooks.gcsSourceBucketName
                                     + "/" + PluginPropertyUtils.pluginProp(path));
  }

  @Then("Enter GCSMove property destination path")
  public void enterGCSMovePropertyDestinationPath() {
    GCSMoveActions.enterDestinationPath("gs://" + TestSetupHooks.gcsTargetBucketName);
  }

  @Then("Enter GCSMove property destination path {string}")
  public void enterGCSMovePropertyDestinationPath(String path) {
    GCSMoveActions.enterDestinationPath("gs://" + TestSetupHooks.gcsTargetBucketName
                                          + "/" + PluginPropertyUtils.pluginProp(path));
  }

  @Then("Enter GCS Move properties with blank property {string}")
  public void enterGCSMovePropertiesWithBlankProperty(String property) {
    if (!property.equalsIgnoreCase("sourcePath")) {
      GCSMoveActions.enterSourcePath(PluginPropertyUtils.pluginProp("gcsMoveValidGcsPath"));
    }
    if (!property.equalsIgnoreCase("destPath")) {
      GCSMoveActions.enterDestinationPath(PluginPropertyUtils.pluginProp("gcsMoveValidGcsPath"));
    }
    if (!PluginPropertyUtils.pluginProp("gcsMoveMandatoryProperties").contains(property)) {
      Assert.fail("Invalid GCSMove mandatory property " + property);
    }
  }

  @Then("Enter GCSMove property encryption key name {string} if cmek is enabled")
  public void enterGCSMovePropertyEncryptionKeyNameIfCmekIsEnabled(String cmek) {
    String cmekGCS = PluginPropertyUtils.pluginProp(cmek);
    if (cmekGCS != null) {
      GCSMoveActions.enterEncryptionKeyName(cmekGCS);
      BeforeActions.scenario.write("Entered encryption key name - " + cmekGCS);
    }
  }

  @Then("Select GCSMove property move all subdirectories as {string}")
  public void selectGCSMovePropertyMoveAllSubdirectoriesAs(String value) {
    GCSMoveActions.selectMoveAllSubdirectories(value);
  }

  @Then("Select GCSMove property overwrite existing files as {string}")
  public void selectGCSMovePropertyOverwriteExistingFilesAs(String value) {
    GCSMoveActions.selectOverwriteExistingFiles(value);
  }

  @Then("Enter GCSMove property location {string}")
  public void enterGCSMovePropertyLocation(String location) {
    GCSMoveActions.enterLocation(PluginPropertyUtils.pluginProp(location));
  }

  @Then("Close GCSMove properties")
  public void closeGCSMoveProperties() {
    CdfStudioActions.clickCloseButton();
  }

  @Then("Verify GCS Move property {string} invalid bucket name error message is displayed for bucket {string}")
  public void verifyGCSMovePropertyInvalidBucketNameErrorMessageIsDisplayedForBucket(String property,
                                                                                     String bucketName) {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_GCS_INVALID_BUCKET_NAME)
      .replace("BUCKET_NAME", "/" + PluginPropertyUtils.pluginProp(bucketName));
    String actualErrorMessage = PluginPropertyUtils.findPropertyErrorElement(property).getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  @Then("Validate GCSMove successfully moved object {string} to destination bucket")
  public void validateGCSMoveSuccessfullyMovedObjectToDestinationBucket(String path) throws IOException {
    String sourceGCSBucket = TestSetupHooks.gcsSourceBucketName;
    String gcsObject = PluginPropertyUtils.pluginProp(path);
    for (Blob blob : StorageClient.listObjects(sourceGCSBucket).iterateAll()) {
      if (blob.getName().contains(gcsObject)) {
        Assert.fail("Object is not moved from source bucket" + sourceGCSBucket);
        return;
      }
    }
    BeforeActions.scenario.write("Object is moved from source GCS Bucket " + sourceGCSBucket + " successfully");
    String targetGCSBucket = TestSetupHooks.gcsTargetBucketName;
    try {
      for (Blob blob : StorageClient.listObjects(targetGCSBucket).iterateAll()) {
        if (blob.getName().contains(gcsObject)) {
          BeforeActions.scenario.write("Object moved to gcs bucket " + targetGCSBucket + " successfully");
          return;
        }
      }
      Assert.fail("Object not moved to target gcs bucket " + targetGCSBucket);
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        Assert.fail("Target gcs bucket " + targetGCSBucket + " not created - " + e.getMessage());
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Then("Validate GCSMove failed to move object {string} to destination bucket")
  public void validateGCSMoveFailedToMoveObjectToDestinationBucket(String path) throws IOException {
    String sourceGCSBucket = TestSetupHooks.gcsSourceBucketName;
    String gcsObject = PluginPropertyUtils.pluginProp(path);
    for (Blob blob : StorageClient.listObjects(sourceGCSBucket).iterateAll()) {
      if (blob.getName().contains(gcsObject)) {
        BeforeActions.scenario.write("Object is not moved from source bucket" + sourceGCSBucket);
        return;
      }
    }
    Assert.fail("Object is deleted from source GCS Bucket " + sourceGCSBucket);
  }

  @Then("Validate GCSMove did not move subdirectory {string} to destination bucket")
  public void validateGCSMoveDidNotMoveSubdirectoryToDestinationBucket(String subdirectory) throws IOException {
    String sourceGCSBucket = TestSetupHooks.gcsSourceBucketName;
    String gcsObject = PluginPropertyUtils.pluginProp(subdirectory);
    for (Blob blob : StorageClient.listObjects(sourceGCSBucket).iterateAll()) {
      if (blob.getName().contains(gcsObject)) {
        BeforeActions.scenario.write("Subdirectory is not moved from source bucket" + sourceGCSBucket);
        return;
      }
    }
    Assert.fail("Subdirectory is moved from source GCS Bucket " + sourceGCSBucket);
  }

  @Then("Enter GCSMove property {string} as macro argument {string}")
  public void enterGCSMovePropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    enterPropertyAsMacroArgument(pluginProperty, macroArgument);
  }

  @Then("Enter GCSMove cmek property {string} as macro argument {string} if cmek is enabled")
  public void enterGCSMoveCmekPropertyAsMacroArgumentIfCmekIsEnabled(String pluginProperty, String macroArgument) {
    String cmekGCS = PluginPropertyUtils.pluginProp("cmekGCS");
    if (cmekGCS != null) {
      enterPropertyAsMacroArgument(pluginProperty, macroArgument);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Enter runtime argument value {string} for GCSMove property sourcePath key {string}")
  public void enterRuntimeArgumentValueForGCSMovePropertySourcePathKey(String value, String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey),
                           "gs://" + TestSetupHooks.gcsSourceBucketName + "/" + PluginPropertyUtils.pluginProp(value));
  }

  @Then("Enter runtime argument value for GCSMove property destination path key {string}")
  public void enterRuntimeArgumentValueForGCSMovePropertyDestinationPathKey(String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey),
                           "gs://" + TestSetupHooks.gcsTargetBucketName);
  }

  @Then("Enter runtime argument value {string} for GCSMove cmek property key {string} if GCS cmek is enabled")
  public void enterRuntimeArgumentValueForGCSMoveCmekPropertyKeyIfGCSCmekIsEnabled(String value,
                                                                                   String runtimeArgumentKey) {
    String cmekGCS = PluginPropertyUtils.pluginProp(value);
    if (cmekGCS != null) {
      ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey), cmekGCS);
      BeforeActions.scenario.write("GCS encryption key name - " + cmekGCS);
      return;
    }
    BeforeActions.scenario.write("CMEK not enabled");
  }

  @Then("Validate GCSMove successfully moved object {string} to destination bucket in location {string}")
  public void validateGCSMoveSuccessfullyMovedObjectToDestinationBucketInLocation(String path, String location) {
    String targetGCSBucket = TestSetupHooks.gcsTargetBucketName;
    try {
      if (StorageClient.getBucketMetadata(targetGCSBucket).getLocation().equalsIgnoreCase(PluginPropertyUtils.pluginProp
        (location))) {
        validateGCSMoveSuccessfullyMovedObjectToDestinationBucket(path);
        return;
      }
      Assert.fail("Target gcs bucket " + targetGCSBucket + " is not created in location " + location);
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        Assert.fail("Target gcs bucket " + targetGCSBucket + " not created - " + e.getMessage());
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Then("Enter GCSMove property source path")
  public void enterGCSMovePropertySourcePath() {
    GCSMoveActions.enterSourcePath("gs://" + TestSetupHooks.gcsSourceBucketName);
  }

  @Then("Verify whether the {string} object present in destination bucket")
  public void verifyWhetherTheObjectPresentInDestinationBucket(String path) throws IOException {
    String targetGCSBucket = TestSetupHooks.gcsTargetBucketName;
    String gcsObject = PluginPropertyUtils.pluginProp(path);

    boolean isObjectPresentInBucket = false;
    for (Blob blob : StorageClient.listObjects(targetGCSBucket).iterateAll()) {
      if (blob.getName().contains(gcsObject)) {
        isObjectPresentInBucket = true;
        break;
      }
    }
    if (isObjectPresentInBucket) {
      BeforeActions.scenario.write("Object is created in source GCS Bucket " + targetGCSBucket + " successfully");
    } else {
      Assert.fail("Object is not created in source bucket" + targetGCSBucket);
    }
  }
}
