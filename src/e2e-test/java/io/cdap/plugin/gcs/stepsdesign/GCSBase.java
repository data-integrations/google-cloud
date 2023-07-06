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
package io.cdap.plugin.gcs.stepsdesign;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfGCSLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.common.ValidationHelper;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.utils.E2EHelper;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Then;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.util.Optional;

/**
 * GCS Plugin related common step design.
 */
public class GCSBase implements E2EHelper {

  static {
    SeleniumHelper.getPropertiesLocators(CdfStudioLocators.class);
  }

  @Then("Enter GCS property reference name")
  public void enterGCSPropertyReferenceName() {
    CdfGcsActions.enterReferenceName();
  }

  @Then("Enter GCS property projectId and reference name")
  public void enterGCSPropertyProjectIdAndReferenceName() throws IOException {
    CdfGcsActions.enterProjectId();
    CdfGcsActions.enterReferenceName();
  }

  @Then("Enter GCS property path {string}")
  public void enterGCSPropertyPath(String path) {
    CdfGcsActions.getGcsBucket(PluginPropertyUtils.pluginProp(path));
  }

  @Then("Select GCS property format {string}")
  public void selectGCSPropertyFormat(String format) throws InterruptedException {
    CdfGcsActions.selectFormat(format);
  }

  @Then("Enter GCS property delimiter {string}")
  public void enterGCSPropertyDelimiter(String delimiter) {
    CdfGcsActions.enterDelimiterField(PluginPropertyUtils.pluginProp(delimiter));
  }

  @Then("Close the GCS properties")
  public void closeTheGCSProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Enter the GCS properties with blank property {string}")
  public void enterTheGCSPropertiesWithBlankProperty(String property) throws InterruptedException {
    if (!property.equalsIgnoreCase("referenceName")) {
      CdfGcsActions.enterReferenceName();
    }
    if (!property.equalsIgnoreCase("path")) {
      CdfGcsActions.getGcsBucket("gs://cdf-athena-test/dummy");
    }
    if (!property.equalsIgnoreCase("format")) {
      CdfGcsActions.selectFormat("csv");
    }
    if (!PluginPropertyUtils.pluginProp("gcsMandatoryProperties").contains(property)) {
      Assert.fail("Invalid GCS mandatory property " + property);
    }
  }

  @Then("Verify invalid bucket name error message is displayed for bucket {string}")
  public void verifyInvalidBucketNameErrorMessageIsDisplayedForBucket(String bucketName) {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton);
    String expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_GCS_INVALID_BUCKET_NAME)
      .replace("BUCKET_NAME", PluginPropertyUtils.pluginProp(bucketName));
    String actualErrorMessage = PluginPropertyUtils.findPropertyErrorElement("path").getText();
    Assert.assertEquals("Invalid bucket name error message should be displayed",
                        expectedErrorMessage, actualErrorMessage);
    String actualColor = PluginPropertyUtils.getErrorColor(PluginPropertyUtils.findPropertyErrorElement("path"));
    String expectedColor = ConstantsUtil.ERROR_MSG_COLOR;
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Verify get schema fails with error")
  public void verifyGetSchemaFailsWithError() {
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfGCSLocators.getSchemaLoadComplete, 10L);
    String expectedErrorMessage = PluginPropertyUtils.errorProp(ConstantsUtil.VALIDATION_ERROR_MESSAGE)
      .replace("COUNT", "1").replace("ERROR", "error");
    String actualErrorMessage = CdfStudioLocators.pluginValidationErrorMsg.getText();
    Assert.assertEquals("Validation error message should be displayed", expectedErrorMessage, actualErrorMessage);
  }

  @Then("Verify Output Path field Error Message for incorrect path field {string}")
  public void verifyOutputPathFieldErrorMessageForIncorrectPathField(String pathField) {
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfGCSLocators.getSchemaLoadComplete, 10L);
    String expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_GCS_INVALID_PATH)
      .replace("PATH_FIELD", PluginPropertyUtils.pluginProp(pathField));
    String actualErrorMessage = PluginPropertyUtils.findPropertyErrorElement("pathField").getText();
    Assert.assertEquals("Invalid path error message should be displayed", expectedErrorMessage, actualErrorMessage);
    String actualColor = PluginPropertyUtils.getErrorColor(PluginPropertyUtils.findPropertyErrorElement("pathField"));
    String expectedColor = ConstantsUtil.ERROR_MSG_COLOR;
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Verify output field {string} in target BigQuery table contains path of the source GcsBucket {string}")
  public void verifyOutputFieldInTargetBigQueryTableContainsPathOfTheGcsBucket(
    String field, String path) throws IOException, InterruptedException {
    Optional<String> result = BigQueryClient
      .getSoleQueryResult("SELECT distinct " + PluginPropertyUtils.pluginProp(field) + " as bucket FROM `"
                            + (PluginPropertyUtils.pluginProp("projectId")) + "."
                            + (PluginPropertyUtils.pluginProp("dataset")) + "."
                            + TestSetupHooks.bqTargetTable + "` ");
    String pathFromBQTable = StringUtils.EMPTY;
    if (result.isPresent()) {
      pathFromBQTable = result.get();
    }
    BeforeActions.scenario.write("GCS bucket path in BQ Table :" + pathFromBQTable);
    Assert.assertEquals("BQ table output field should contain GCS bucket path ",
                        "gs://" + TestSetupHooks.gcsSourceBucketName + "/"
                          + PluginPropertyUtils.pluginProp(path), pathFromBQTable);
  }

  @Then("Verify datatype of field {string} is overridden to data type {string} in target BigQuery table")
  public void verifyDatatypeOfFieldIsOverriddenToDataTypeInTargetBigQueryTable(
    String field, String dataType) throws IOException, InterruptedException {
    Optional<String> result = BigQueryClient
      .getSoleQueryResult("SELECT data_type FROM `" + (PluginPropertyUtils.pluginProp("projectId")) + "."
                            + (PluginPropertyUtils.pluginProp("dataset")) + ".INFORMATION_SCHEMA.COLUMNS` " +
                            "WHERE table_name = '" + TestSetupHooks.bqTargetTable
                            + "' and column_name = '" + PluginPropertyUtils.pluginProp(field) + "' ");
    String dataTypeInTargetTable = StringUtils.EMPTY;
    if (result.isPresent()) {
      dataTypeInTargetTable = result.get();
    }
    BeforeActions.scenario.write("Data type in target BQ Table :" + dataTypeInTargetTable);
    Assert.assertEquals(PluginPropertyUtils.pluginProp(dataType),
                        dataTypeInTargetTable.replace("64", StringUtils.EMPTY).toLowerCase());
  }

  @Then("Verify data is transferred to target GCS bucket")
  public void verifyDataIsTransferredToTargetGCSBucket() {
    String gcsBucket = TestSetupHooks.gcsTargetBucketName;
    boolean flag = false;
    try {
      for (Blob blob : StorageClient.listObjects(gcsBucket).iterateAll()) {
        if (blob.getName().contains("part")) {
          flag = true;
          break;
        }
      }
      if (flag) {
        BeforeActions.scenario.write("Data transferred to gcs bucket " + gcsBucket + " successfully");
      } else {
        Assert.fail("Data not transferred to target gcs bucket " + gcsBucket);
      }
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        Assert.fail("Target gcs bucket " + gcsBucket + " not created - " + e.getMessage());
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Then("Enter GCS property {string} as macro argument {string}")
  public void enterGCSPropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    enterPropertyAsMacroArgument(pluginProperty, macroArgument);
  }

  @Then("Verify output field {string} in target BigQuery table contains filename of the source GcsBucket {string}")
  public void verifyOutputFieldInTargetBigQueryTableContainsFilenameOfTheSourceGcsBucket(String field, String path)
    throws IOException, InterruptedException {
    Optional<String> result = BigQueryClient
      .getSoleQueryResult("SELECT distinct " + PluginPropertyUtils.pluginProp(field) + " as bucket FROM `"
                            + (PluginPropertyUtils.pluginProp("projectId")) + "."
                            + (PluginPropertyUtils.pluginProp("dataset")) + "."
                            + TestSetupHooks.bqTargetTable + "` ");
    String pathFromBQTable = StringUtils.EMPTY;
    if (result.isPresent()) {
      pathFromBQTable = result.get();
    }
    BeforeActions.scenario.write("GCS bucket filename in BQ Table :" + pathFromBQTable);
    String filename[] = PluginPropertyUtils.pluginProp(path).split("/");
    Assert.assertEquals("BQ table output field should contain GCS bucket filename ",
                        filename[filename.length - 1], pathFromBQTable);
  }

  @Then("Enter GCS File system properties field {string}")
  public void enterGCSFileSystemPropertiesField(String property) {
    CdfGcsActions.enterFileSystemProperties(PluginPropertyUtils.pluginProp(property));
  }

  @Then("Click on Tidy in GCS File system properties")
  public void clickOnTidyInGCSFileSystemProperties() {
    CdfGcsActions.clickFileSystemPropertiesTidyButton();
  }

  @Then("Verify data is transferred to target GCS bucket with file format {string}")
  public void verifyDataIsTransferredToTargetGCSBucketWithFileFormat(String fileFormat) {
    String gcsBucket = TestSetupHooks.gcsTargetBucketName;
    if (verifyFilePresentInGcsBucket(gcsBucket, "." + fileFormat, FilePart.SUFFIX)) {
      BeforeActions.scenario.write("Data transferred to gcs bucket " + gcsBucket + " with format "
                                     + fileFormat + " successfully");
    } else {
      Assert.fail("Data not transferred to target gcs bucket " + gcsBucket + " with format " + fileFormat);
    }
  }

  @Then("Verify data is transferred to target GCS bucket with fileName prefix {string}")
  public void verifyDataIsTransferredToTargetGCSBucketWithFileNamePrefix(String fileNamePrefix) {
    String gcsBucket = TestSetupHooks.gcsTargetBucketName;
    fileNamePrefix = PluginPropertyUtils.pluginProp(fileNamePrefix);
    if (verifyFilePresentInGcsBucket(gcsBucket, fileNamePrefix, FilePart.PREFIX)) {
      BeforeActions.scenario.write("Data transferred to gcs bucket " + gcsBucket + " with fileNamePrefix "
                                     + fileNamePrefix + " successfully");
    } else {
      Assert.fail("Data not transferred to target gcs bucket " + gcsBucket +
                    " with fileNamePrefix " + fileNamePrefix);
    }
  }

  @Then("Verify data is transferred to target GCS bucket with path suffix {string}")
  public void verifyDataIsTransferredToTargetGCSBucketWithPathSuffix(String pathSuffix) {
    String gcsBucket = TestSetupHooks.gcsTargetBucketName;
    pathSuffix = PluginPropertyUtils.pluginProp(pathSuffix);
    if (verifyFilePresentInGcsBucket(gcsBucket, pathSuffix, FilePart.SUFFIX)) {
      BeforeActions.scenario.write("Data transferred to gcs bucket " + gcsBucket + " with pathSuffix "
                                     + pathSuffix + " successfully");
    } else {
      Assert.fail("Data not transferred to target gcs bucket " + gcsBucket +
                    " with pathSuffix " + pathSuffix);
    }
  }

  private boolean verifyFilePresentInGcsBucket(String gcsBucket, String filename, FilePart filePart) {
    try {
      for (Blob blob : StorageClient.listObjects(gcsBucket).iterateAll()) {
        String blobFilePath[] = blob.getName().split("/");
        String blobFileName = blobFilePath[blobFilePath.length - 1];
        if (filePart.equals(FilePart.PREFIX)) {
          if (blobFileName.startsWith(filename)) {
            return true;
          }
        }
        if (filePart.equals(FilePart.SUFFIX)) {
          if (blobFileName.endsWith(filename)) {
            return true;
          }
        }
        if (filePart.equals(FilePart.CONTAINS)) {
          if (blobFileName.contains(filename)) {
            return true;
          }
        }
        if (filePart.equals(FilePart.EQUALS)) {
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

  @Then("Validate the values of records transferred to GCS bucket is equal to the values from source BigQuery table")
  public void validateTheValuesOfRecordsTransferredToGcsBucketIsEqualToTheValuesFromSourceBigQueryTable()
    throws InterruptedException, IOException {
    int sourceBQRecordsCount = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp("bqSourceTable"));
    BeforeActions.scenario.write("No of Records from source BigQuery table:" + sourceBQRecordsCount);
    Assert.assertEquals("Out records should match with GCS file records count",
                        CdfPipelineRunAction.getCountDisplayedOnSourcePluginAsRecordsOut(), sourceBQRecordsCount);

    boolean recordsMatched = ValidationHelper.validateBQDataToGCS(
      TestSetupHooks.bqSourceTable, TestSetupHooks.gcsTargetBucketName);
    Assert.assertTrue("Value of records transferred to the GCS bucket file should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }

  @Then("Validate the values of records transferred from GCS bucket file is equal to the values of " +
    "target BigQuery table")
  public void validateTheValuesOfRecordsTransferredFromGcsBucketFileIsEqualToTheValuesOfTargetBigQueryTable()
    throws InterruptedException, IOException {
    int targetBQRecordsCount = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp("bqTargetTable"));
    BeforeActions.scenario.write("No of Records from source BigQuery table:" + targetBQRecordsCount);
    Assert.assertEquals("Out records should match with GCS file records count",
                        CdfPipelineRunAction.getCountDisplayedOnSourcePluginAsRecordsOut(), targetBQRecordsCount);

    boolean recordsMatched = ValidationHelper.validateGCSDataToBQ(
      TestSetupHooks.gcsSourceBucketName, TestSetupHooks.bqTargetTable);
    Assert.assertTrue("Value of records transferred to the GCS bucket file should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }
}
