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
package io.cdap.plugin.gcsmultifile.stepsdesign;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.gcsmultifile.actions.GCSMultiFileActions;
import io.cdap.plugin.gcsmultifile.locators.GCSMultiFileLocators;
import io.cdap.plugin.utils.E2EHelper;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;

/**
 * GCSMultiFile connector related design.
 */
public class GCSMultiFileConnector implements E2EHelper {

  @Then("Close GCSMultiFile Properties")
  public void closeGCSMultiFileProperties() {
    GCSMultiFileActions.closeGcsMultiFile();
  }

  @Then("Verify GCSMultiFile Content Type validation error")
  public void verifyGCSMultiFileContentTypeValidationError() {
    CdfStudioActions.clickValidateButton();
    String expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_GCS_MULTIFILE_CONTENTTYPE);
    String actualErrorMessage = PluginPropertyUtils.findPropertyErrorElement("contentType").getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

  @Then("Enter the GCSMultiFile properties with blank property {string}")
  public void enterTheGCSMultiFilePropertiesWithBlankProperty(String property) throws IOException {
    if (property.equalsIgnoreCase("referenceName")) {
      GCSMultiFileActions.enterGcsMultiFilepath(PluginPropertyUtils.pluginProp("multiFileGcsPath"));
    } else if (property.equalsIgnoreCase("path")) {
      GCSMultiFileActions.enterReferenceName();
    } else if (property.equalsIgnoreCase("splitField")) {
      GCSMultiFileActions.enterReferenceName();
      GCSMultiFileActions.enterGcsMultiFilepath(PluginPropertyUtils.pluginProp("multiFileGcsPath"));
      ElementHelper.replaceElementValue(GCSMultiFileLocators.splitField, "");
    } else {
      Assert.fail("Invalid GCSMultiFile Mandatory Field " + property);
    }
  }

  @Then("Verify GCSMultiFile invalid path name {string}")
  public void verifyGCSMultiFileInvalidPathName(String path) {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton);
    String expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_GCS_INVALID_BUCKET_NAME)
      .replace("BUCKET_NAME", PluginPropertyUtils.pluginProp(path));
    String actualErrorMessage = PluginPropertyUtils.findPropertyErrorElement("path").getText();
    Assert.assertEquals("Invalid bucket name error message should be displayed",
                        expectedErrorMessage, actualErrorMessage);
    String actualColor = PluginPropertyUtils.getErrorColor(PluginPropertyUtils.findPropertyErrorElement("path"));
    String expectedColor = ConstantsUtil.ERROR_MSG_COLOR;
    Assert.assertEquals(expectedColor, actualColor);
  }

  @When("Sink is GCSMultiFile")
  public void sinkIsGCSMultiFile() {
    CdfStudioActions.clickSink();
    selectSinkPlugin("GCSMultiFiles");
  }

  @Then("Open GCSMultiFile sink properties")
  public void openGCSMultiFileSinkProperties() {
    openSinkPluginProperties("GCSMultiFiles");
  }

  @Then("Enter GCSMultiFile property projectId {string}")
  public void enterGCSMultiFilePropertyProjectId(String projectId) throws IOException {
    GCSMultiFileActions.enterProjectId(PluginPropertyUtils.pluginProp(projectId));
  }

  @Then("Enter GCSMultiFile property reference name")
  public void enterGCSMultiFilePropertyReferenceName() {
    GCSMultiFileActions.enterReferenceName();
  }

  @Then("Select GCSMultiFile property format {string}")
  public void selectGCSMultiFilePropertyFormat(String format) throws InterruptedException {
    GCSMultiFileActions.selectFormat(format);
  }

  @Then("Enter GCSMultiFile property path")
  public void enterGCSMultiFilePropertyPath() {
    CdfGcsActions.getGcsBucket("gs://" + TestSetupHooks.gcsTargetBucketName);
  }

  @Then("Enter GCSMultiFile property path {string}")
  public void enterGCSMultiFilePropertyPath(String path) {
    CdfGcsActions.getGcsBucket(PluginPropertyUtils.pluginProp(path));
  }

  @Then("Select GCSMultiFile property ContentType {string}")
  public void selectGCSMultiFilePropertyContentType(String contentType) throws InterruptedException {
    GCSMultiFileActions.selectContentType(contentType);
  }

  @Then("Select GCSMultiFile property CodecType {string}")
  public void selectGCSMultiFilePropertyCodecType(String codecType) throws InterruptedException {
    GCSMultiFileActions.selectCodec(PluginPropertyUtils.pluginProp(codecType));
  }

  @Then("Enter GCSMultiFile property delimiter {string}")
  public void enterGCSMultiFilePropertyDelimiter(String delimiter) {
    GCSMultiFileActions.enterDelimiter(PluginPropertyUtils.pluginProp(delimiter));
  }

  @Then("Enter GCSMultiFile property split field {string}")
  public void enterGCSMultiFilePropertySplitField(String splitField) throws IOException {
    GCSMultiFileActions.enterSplitField(PluginPropertyUtils.pluginProp(splitField));
  }

  @Then("Enter GCSMultiFile sink property path suffix {string}")
  public void enterGCSMultiFileSinkPropertyPathSuffix(String pathSuffix) {
    CdfGcsActions.enterPathSuffix(PluginPropertyUtils.pluginProp(pathSuffix));
  }

  @Then("Toggle GCSMultiFile property allow flexible schemas to true")
  public void toggleGCSMultiFilePropertyAllowFlexibleSchemasToTrue() throws IOException {
    GCSMultiFileActions.selectAllowFlexibleSchema();
  }

  @Then("Verify data is transferred to target GCSMultiFile bucket with path suffix {string}")
  public void verifyDataIsTransferredToTargetGCSMultiFileBucketWithPathSuffix(String pathSuffix) {
    String gcsBucket = TestSetupHooks.gcsTargetBucketName;
    pathSuffix = PluginPropertyUtils.pluginProp(pathSuffix);
    if (verifyFilePresentInGcsBucket(gcsBucket, pathSuffix, "suffix")) {
      BeforeActions.scenario.write("Data transferred to gcs bucket " + gcsBucket + " with pathSuffix "
                                     + pathSuffix + " successfully");
    } else {
      Assert.fail("Data not transferred to target gcs bucket " + gcsBucket +
                    " with pathSuffix " + pathSuffix);
    }
  }

  @Then("Enter GCSMultiFile property {string} as macro argument {string}")
  public void enterGCSMultiFilePropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    enterPropertyAsMacroArgument(pluginProperty, macroArgument);
  }

  @Then("Enter runtime argument value for GCSMultiFile property path key {string}")
  public void enterRuntimeArgumentValueForGCSMultiFilePropertyPathKey(String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey),
                           "gs://" + TestSetupHooks.gcsTargetBucketName);
  }

  @Then("Verify data is transferred to target GCS bucket with file format {string}")
  public void verifyDataIsTransferredToTargetGCSBucketWithFileFormat(String fileFormat) {
    String gcsBucket = TestSetupHooks.gcsTargetBucketName;
    if (verifyFilePresentInGcsBucket(gcsBucket, "." + fileFormat, "suffix")) {
      BeforeActions.scenario.write("Data transferred to gcs bucket " + gcsBucket + " with format "
                                     + fileFormat + " successfully");
    } else {
      Assert.fail("Data not transferred to target gcs bucket " + gcsBucket + " with format " + fileFormat);
    }
  }

  @Then("Verify folders with split field are created in GCS bucket for tables {string}")
  public void verifyFoldersWithSplitFieldAreCreatedInGCSBucketForTables(String tableNames) {
    String gcsBucket = TestSetupHooks.gcsTargetBucketName;
    String[] tableNamesList = PluginPropertyUtils.pluginProp(tableNames).split(",");
    for (String tableName : tableNamesList) {
      if (verifyFilePresentInGcsBucket(gcsBucket, tableName, "contains")) {
        BeforeActions.scenario.write("Folder created in gcs bucket " + gcsBucket + " with tablename "
                                       + tableName + " successfully");
      } else {
        Assert.fail("Folder not created in gcs bucket " + gcsBucket +
                      " with tablename " + tableName);
      }
    }
  }

  private boolean verifyFilePresentInGcsBucket(String gcsBucket, String filename, String filePart) {
    try {
      for (Blob blob : StorageClient.listObjects(gcsBucket).iterateAll()) {
        String blobFilePath[] = blob.getName().split("/");
        String blobFileName = blobFilePath[blobFilePath.length - 1];
        if (filePart.equalsIgnoreCase(FilePart.PREFIX.filePart)) {
          if (blobFileName.startsWith(filename)) {
            return true;
          }
        }
        if (filePart.equalsIgnoreCase(FilePart.SUFFIX.filePart)) {
          if (blobFileName.endsWith(filename)) {
            return true;
          }
        }
        if (filePart.equalsIgnoreCase(FilePart.CONTAINS.filePart)) {
          if (blobFileName.contains(filename)) {
            return true;
          }
        }
        if (filePart.equalsIgnoreCase(FilePart.EQUALS.filePart)) {
          if (blobFileName.equals(filename)) {
            return true;
          }
        }
      }
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        Assert.fail("Target gcs bucket " + gcsBucket + " not created - " + e.getMessage());
      } else {
        Assert.fail(e.getMessage());
      }
    }
    return false;
  }

  private enum FilePart {
    PREFIX("prefix"),
    SUFFIX("suffix"),
    CONTAINS("contains"),
    EQUALS("equals");

    FilePart(String filePart) {
      this.filePart = filePart;
    }

    String filePart;
  }
}
