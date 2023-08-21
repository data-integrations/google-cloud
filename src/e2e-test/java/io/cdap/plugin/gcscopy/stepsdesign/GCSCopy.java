
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

package io.cdap.plugin.gcscopy.stepsdesign;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.gcscopy.actions.GCSCopyActions;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


/**
 * GCS Copy plugin related stepsdesign.
 */

public class GCSCopy {

  @Then("Validate GCSCopy copies subdirectories along with its files to the destination bucket")
  public static boolean compareBuckets() throws IOException {
    // Initialize the GCS client
    Boolean bucketMatched = false;
    Storage storage = StorageOptions.newBuilder().setProjectId(
      PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID)).build().getService();
    // Get references to the source and destination buckets
    String sourceGCSBucket = TestSetupHooks.gcsSourceBucketName;
    String targetGCSBucket = TestSetupHooks.gcsTargetBucketName;
    Bucket sourceBucket = storage.get(sourceGCSBucket);
    Bucket destinationBucket = storage.get(targetGCSBucket);
    // List objects in the source bucket
    Set<String> sourceObjectNames = new HashSet<>();
    for (Blob blob : sourceBucket.list(Storage.BlobListOption.prefix(PluginPropertyUtils.pluginProp
      ("gcsCopyReadRecursivePath"))).iterateAll()) {
      sourceObjectNames.add(blob.getName());
    }
    // List objects in the destination bucket
    Set<String> destinationObjectNames = new HashSet<>();
    for (Blob blob : destinationBucket.list(Storage.BlobListOption.prefix(PluginPropertyUtils.pluginProp
      ("gcsCopyReadRecursivePath"))).iterateAll()) {
      destinationObjectNames.add(blob.getName());
    }
    try {
      if (sourceObjectNames.equals(destinationObjectNames)) {
        BeforeActions.scenario.write("Subdirectory along with its files is copied " + targetGCSBucket +
                                       " successfully");
        return bucketMatched;
      }
      Assert.fail("Object not copied to target gcs bucket " + targetGCSBucket);
    } catch (StorageException e) {
      if (e.getMessage().equals("The specified bucket does not exist")) {
        Assert.fail("Target gcs bucket " + targetGCSBucket + " not created - " + e.getMessage());
      } else {
        Assert.fail(e.getMessage());
      }
    }
    return false;
  }


  @Then("Validate GCSCopy did not copy subdirectories along with its files to the destination bucket")
  public static void compareBucketsTarget() throws IOException {
    // Initialize the GCS client
    Storage storage = StorageOptions.newBuilder().setProjectId(
      PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID)).build().getService();
    // Get references to the source and destination buckets
    String sourceGCSBucket = TestSetupHooks.gcsSourceBucketName;
    String targetGCSBucket = TestSetupHooks.gcsTargetBucketName;
    Bucket sourceBucket = storage.get(sourceGCSBucket);
    Bucket destinationBucket = storage.get(targetGCSBucket);
    // List objects in the source bucket
    Set<String> sourceObjectNames = new HashSet<>();
    for (Blob blob : sourceBucket.list(Storage.BlobListOption.prefix(PluginPropertyUtils.pluginProp
      ("gcsCopyReadRecursivePath"))).iterateAll()) {
      sourceObjectNames.add(blob.getName());
    }
    // List objects in the destination bucket
    Set<String> destinationObjectNames = new HashSet<>();
    for (Blob blob : destinationBucket.list(Storage.BlobListOption.prefix(PluginPropertyUtils.pluginProp
      ("gcsCopyReadRecursivePath"))).iterateAll()) {
      destinationObjectNames.add(blob.getName());
    }
    try {
      if (destinationObjectNames.isEmpty()) {
        BeforeActions.scenario.write("Target bucket is empty , no files copied from subdirectory "
                                       + targetGCSBucket + " successfully");
        return;
      }
      Assert.fail("Object copied to target gcs bucket along with its files " + targetGCSBucket);
    } catch (StorageException e) {
      if (e.getMessage().equals("The specified bucket does not exist")) {
        Assert.fail("Target gcs bucket " + targetGCSBucket + " not created - " + e.getMessage());
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Then("Validate GCSCopy successfully copies object {string} to destination bucket")
  public void validateGCSCopySuccessfullyCopiedObjectToDestinationBucket(String path) throws IOException {
    String sourceGCSBucket = TestSetupHooks.gcsSourceBucketName;
    String gcsObject = PluginPropertyUtils.pluginProp(path);
    boolean isPresentAtSource = false;
    for (Blob blob : StorageClient.listObjects(sourceGCSBucket).iterateAll()) {
      if (blob.getName().equals(gcsObject)) {
        isPresentAtSource = true;
        break;
      }
    }
    if (!isPresentAtSource) {

      Assert.fail("Object is not present in source bucket" + sourceGCSBucket);
    }
    BeforeActions.scenario.write("Object is copied from source GCS Bucket " + sourceGCSBucket + " successfully");
    String targetGCSBucket = TestSetupHooks.gcsTargetBucketName;
    try {
      for (Blob blob : StorageClient.listObjects(targetGCSBucket).iterateAll()) {
        if (blob.getName().equals(gcsObject)) {
          BeforeActions.scenario.write("Object copied to gcs bucket " + targetGCSBucket + " successfully");
          return;
        }
      }
      Assert.fail("Object not copied to target gcs bucket " + targetGCSBucket);
    } catch (StorageException | IOException e) {
      if (e.getMessage().equals("The specified bucket does not exist")) {
        Assert.fail("Target gcs bucket " + targetGCSBucket + " not created - " + e.getMessage());
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Then("Enter GCSCopy property encryption key name {string} if cmek is enabled")
  public void enterGCSCopyPropertyEncryptionKeyNameIfCmekIsEnabled(String cmek) {
    String cmekGCS = PluginPropertyUtils.pluginProp(cmek);
    if (cmekGCS != null) {
      GCSCopyActions.enterEncryptionKeyName(cmekGCS);
      BeforeActions.scenario.write("Entered encryption key name - " + cmekGCS);
    }
  }

  @Then("Enter GCSCopy property source path {string}")
  public void enterGCSCopyPropertySourcePath(String path) {
    GCSCopyActions.enterSourcePath("gs://" + TestSetupHooks.gcsSourceBucketName
                                     + "/" + PluginPropertyUtils.pluginProp(path));
  }

  @Then("Enter GCSCopy property destination path")
  public void enterGCSCopyPropertyDestinationPath() {
    GCSCopyActions.enterDestinationPath("gs://" + TestSetupHooks.gcsTargetBucketName);
  }

  @Then("Enter GCSCopy property destination path {string}")
  public void enterGCSCopyPropertyDestinationPath(String path) {
    GCSCopyActions.enterDestinationPath("gs://" + TestSetupHooks.gcsTargetBucketName
                                          + "/" + PluginPropertyUtils.pluginProp(path));
  }

  @Then("Validate GCSCopy successfully copied object {string} to destination bucket in location {string}")
  public void validateGCSCopySuccessfullyCopiedObjectToDestinationBucketInLocation(String path, String location) {
    String targetGCSBucket = TestSetupHooks.gcsTargetBucketName;
    try {
      if (StorageClient.getBucketMetadata(targetGCSBucket).getLocation().equalsIgnoreCase(PluginPropertyUtils.pluginProp
        (location))) {
        validateGCSCopySuccessfullyCopiedObjectToDestinationBucket(path);
        return;
      }
      Assert.fail("Target gcs bucket " + targetGCSBucket + " is not created in location " + location);
    } catch (StorageException | IOException e) {
      if (e.getMessage().equals("The specified bucket does not exist")) {
        Assert.fail("Target gcs bucket " + targetGCSBucket + " not created - " + e.getMessage());
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Then("Enter runtime argument value {string} for GCSCopy property sourcePath key {string}")
  public void enterRuntimeArgumentValueForGCSCopyPropertySourcePathKey(String value, String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey),
                           "gs://" + TestSetupHooks.gcsSourceBucketName + "/" + PluginPropertyUtils.pluginProp(value));
  }

  @Then("Enter runtime argument value for GCSCopy property destination path key {string}")
  public void enterRuntimeArgumentValueForGCSCopyPropertyDestinationPathKey(String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey),
                           "gs://" + TestSetupHooks.gcsTargetBucketName);
  }
  @Then("Validate GCSCopy failed to copy object {string} to destination bucket")
  public void validateGCSCopyFailedToCopyObjectToDestinationBucket(String path) throws IOException {
    String sourceGCSBucket = TestSetupHooks.gcsSourceBucketName;
    String gcsObject = PluginPropertyUtils.pluginProp(path);
    for (Blob blob : StorageClient.listObjects(sourceGCSBucket).iterateAll()) {
      if (blob.getName().equals(gcsObject)) {
        BeforeActions.scenario.write("Object is not copied from source bucket" + sourceGCSBucket);
        return;
      }
    }
    Assert.fail("Object is deleted from source GCS Bucket " + sourceGCSBucket);
  }

  @Then("Validate the data of GCS Copy source bucket and destination bucket {string}")
  public void validateTheDataFromGCSSourceToGCSSinkWithExpectedCsvFileAndTargetGCSBucket(String path) {
    GCSCopyValidation.validateGCSSourceToGCSSinkWithCSVFormat(TestSetupHooks.gcsTargetBucketName,
                                                              PluginPropertyUtils.pluginProp(path));
  }
}
