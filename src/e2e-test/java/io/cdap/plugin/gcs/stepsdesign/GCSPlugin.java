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
import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfGCSLocators;
import io.cdap.e2e.pages.locators.CdfSchemaLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.JsonUtils;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * GCS Plugin related step design.
 */
public class GCSPlugin implements CdfHelper {

  static {
    SeleniumHelper.getPropertiesLocators(CdfStudioLocators.class);
  }

  List<String> propertiesSchemaColumnList = new ArrayList<>();
  Map<String, String> sourcePropertiesOutputSchema = new HashMap<>();
  String cmekGCS =  PluginPropertyUtils.pluginProp(E2ETestConstants.CMEK_GCS);

  @Given("Open Datafusion Project to configure pipeline")
  public void openDatafusionProjectToConfigurePipeline() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source is GCS bucket")
  public void sourceIsGCSBucket() {
    selectSourcePlugin("GCSFile");
  }

  @When("Source is BigQuery")
  public void sourceIsBigQuery() {
    selectSourcePlugin("BigQueryTable");
  }

  @When("Target is GCS bucket")
  public void targetIsGCSBucket() {
    CdfStudioActions.clickSink();
    selectSinkPlugin("GCS");
  }

  @When("Target is BigQuery")
  public void targetIsBigQuery() {
    CdfStudioActions.clickSink();
    selectSinkPlugin("BigQueryTable");
  }

  @Then("Open GCS source properties")
  public void openGCSSourceProperties() {
    openSourcePluginProperties("GCS");
  }

  @Then("Open GCS Sink Properties")
  public void openGCSSinkProperties() {
    openSinkPluginProperties("GCS");
  }

  @Then("Connect source as {string} and sink as {string} to establish connection")
  public void connectSourceAndSinkToEstablishConnection(String source, String sink) {
    connectSourceAndSink(source, sink);
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

  @Then("Enter GCS source property path {string}")
  public void enterGCSSourcePropertyPath(String path) {
    CdfGcsActions.getGcsBucket("gs://" + TestSetupHooks.gcsSourceBucketName + "/"
                                 + PluginPropertyUtils.pluginProp(path));
  }

  @Then("Enter GCS sink property path")
  public void enterGCSSinkPropertyPath() {
    CdfGcsActions.getGcsBucket("gs://" + TestSetupHooks.gcsTargetBucketName);
  }

  @Then("Select GCS property format {string}")
  public void selectGCSPropertyFormat(String format) throws InterruptedException {
    CdfGcsActions.selectFormat(format);
  }

  @Then("Toggle GCS source property skip header to true")
  public void toggleGCSSourcePropertySkipHeaderToTrue() {
    CdfGcsActions.skipHeader();
  }

  @Then("Enter GCS source property path field {string}")
  public void enterGCSSourcePropertyPathField(String pathField) {
    CdfGcsActions.enterPathField(PluginPropertyUtils.pluginProp(pathField));
  }

  @Then("Enter GCS source property override field {string} and data type {string}")
  public void enterGCSSourcePropertyOverrideFieldAndDataType(String overrideField, String dataType) {
    CdfGcsActions.enterOverride(PluginPropertyUtils.pluginProp(overrideField));
    CdfGcsActions.clickOverrideDataType(PluginPropertyUtils.pluginProp(dataType));
  }

  @Then("Enter GCS property delimiter {string}")
  public void enterGCSPropertyDelimiter(String delimiter) {
    CdfGcsActions.enterDelimiterField(PluginPropertyUtils.pluginProp(delimiter));
  }

  @Then("Enter GCS source property minimum split size {string} and maximum split size {string}")
  public void enterGCSSourcePropertyMinimumSplitSizeAndMaximumSplitSize(String minSplitSize, String maxSplitSize) {
    CdfGcsActions.enterMaxSplitSize(PluginPropertyUtils.pluginProp(minSplitSize));
    CdfGcsActions.enterMinSplitSize(PluginPropertyUtils.pluginProp(maxSplitSize));
  }

  @Then("Enter GCS source property regex path filter {string}")
  public void enterGCSSourcePropertyRegexPathFilter(String regexPathFilter) {
    CdfGcsActions.enterRegexPath(PluginPropertyUtils.pluginProp(regexPathFilter));
  }

  @Then("Enter GCS property encryption key name if cmek is enabled")
  public void enterGCSPropertyEncryptionKeyNameIfCmekIsEnabled() {
    if (cmekGCS != null) {
      CdfGcsActions.enterEncryptionKeyName(cmekGCS);
    }
  }

  @Then("Validate output schema with expectedSchema {string}")
  public void validateOutputSchemaWithExpectedSchema(String schemaJsonArray) {
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfGCSLocators.getSchemaLoadComplete, 20L);
    SeleniumHelper.waitElementIsVisible(CdfSchemaLocators.outputSchemaColumnNames.get(0), 2L);
    Map<String, String> expectedOutputSchema =
      JsonUtils.convertKeyValueJsonArrayToMap(PluginPropertyUtils.pluginProp(schemaJsonArray));
    validateSchema(expectedOutputSchema);
    int index = 0;
    for (WebElement element : CdfSchemaLocators.outputSchemaColumnNames) {
      propertiesSchemaColumnList.add(element.getAttribute("value"));
      sourcePropertiesOutputSchema.put(element.getAttribute("value"),
                                       CdfSchemaLocators.outputSchemaDataTypes.get(index).getAttribute("title"));
      index++;
    }
  }

  @Then("Validate {string} plugin properties")
  public void validatePluginProperties(String plugin) {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.pluginValidationSuccessMsg, 20L);
    String expectedMessage = PluginPropertyUtils.errorProp(E2ETestConstants.VALIDATION_SUCCESS_MESSAGE);
    String actualMessage = CdfStudioLocators.pluginValidationSuccessMsg.getText();
    Assert.assertEquals(plugin + " plugin properties validation success message should be displayed"
      , expectedMessage, actualMessage);
  }

  @Then("Close the GCS Properties")
  public void closeTheGCSProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Open BigQuery sink properties")
  public void openBigQuerySinkProperties() {
    openSinkPluginProperties("BigQuery");
  }

  @Then("Open BigQuery source properties")
  public void openBigQuerySourceProperties() {
    openSourcePluginProperties("BigQuery");
  }

  public void enterTheBigQueryPropertiesForTable(String tableName) throws IOException {
    CdfBigQueryPropertiesActions.enterProjectId(PluginPropertyUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(PluginPropertyUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_Ref_" + UUID.randomUUID());
    CdfBigQueryPropertiesActions.enterBigQueryDataset(PluginPropertyUtils.pluginProp("dataset"));
    CdfBigQueryPropertiesActions.enterBigQueryTable(tableName);
  }

  @Then("Enter the BigQuery sink properties")
  public void enterTheBigQuerySinkProperties() throws IOException {
    enterTheBigQueryPropertiesForTable(TestSetupHooks.gcsBqTargetTable);
    CdfBigQueryPropertiesActions.clickUpdateTable();
    CdfBigQueryPropertiesActions.clickTruncatableSwitch();
  }

  @Then("Enter the BigQuery source properties")
  public void enterTheBigQuerySourceProperties() throws IOException {
    enterTheBigQueryPropertiesForTable(TestSetupHooks.gcsBqSourceTable);
  }

  @Then("Close the BigQuery Properties")
  public void closeTheBigQueryProperties() {
    CdfGcsActions.closeButton();
  }


  @Then("Click on preview data for GCS")
  public void clickOnPreviewDataForGCS() {
    openSinkPluginPreviewData("GCS");
  }

  @Then("Click on preview data for BigQuery")
  public void clickOnPreviewDataForBigQuery() {
    openSinkPluginPreviewData("BigQuery");
  }

  @Then("Verify preview output schema matches the outputSchema captured in properties")
  public void verifyPreviewOutputSchemaMatchesTheOutputSchemaCapturedInProperties() {
    List<String> previewSchemaColumnList = new ArrayList<>();
    for (WebElement element : CdfStudioLocators.previewInputRecordColumnNames) {
      previewSchemaColumnList.add(element.getAttribute("title"));
    }
    Assert.assertTrue("Schema column list should be equal to preview column list",
                      previewSchemaColumnList.equals(propertiesSchemaColumnList));
    CdfStudioLocators.previewPropertiesTab.click();
    Map<String, String> previewSinkInputSchema = new HashMap<>();
    int index = 0;
    for (WebElement element : CdfSchemaLocators.inputSchemaColumnNames) {
      previewSinkInputSchema.put(element.getAttribute("value"),
                                 CdfSchemaLocators.inputSchemaDataTypes.get(index).getAttribute("title"));
      index++;
    }
    Assert.assertTrue("Schema should match", previewSinkInputSchema.equals(sourcePropertiesOutputSchema));
  }

  @Then("Get Count of no of records transferred to BigQuery Table")
  public void getCountOfNoOfRecordsTransferredToBigQueryTable() throws IOException, InterruptedException {
    int countRecords = BigQueryClient.countBqQuery(TestSetupHooks.gcsBqTargetTable);
    BeforeActions.scenario.write("**********No of Records Transferred******************:" + countRecords);
    Assert.assertEquals("Number of records transferred should be equal to records out ", countRecords, recordOut());
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

  @Then("Validate mandatory property error for {string}")
  public void validateMandatoryPropertyErrorFor(String property) {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton);
    PluginPropertyUtils.validateMandatoryPropertyError(property);
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
    CdfBigQueryPropertiesActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfGCSLocators.getSchemaLoadComplete, 10L);
    String expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.VALIDATION_ERROR_MESSAGE)
      .replace("COUNT", "1").replace("ERROR", "error");
    String actualErrorMessage = CdfStudioLocators.pluginValidationErrorMsg.getText();
    Assert.assertEquals("Validation error message should be displayed", expectedErrorMessage, actualErrorMessage);
  }

  @Then("Verify GCS plugin properties validation fails with error")
  public void verifyGCSPluginPropertiesValidationFailsWithError() {
    CdfStudioActions.clickValidateButton();
    SeleniumHelper.waitElementIsVisible(CdfStudioLocators.validateButton, 5L);
    String expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.VALIDATION_ERROR_MESSAGE)
      .replace("COUNT", "1").replace("ERROR", "error");
    String actualErrorMessage = CdfStudioLocators.pluginValidationErrorMsg.getText();
    Assert.assertEquals("Validation error message should be displayed", expectedErrorMessage, actualErrorMessage);
  }

  @Then("Verify Output Path field Error Message for incorrect path field {string}")
  public void verifyOutputPathFieldErrorMessageForIncorrectPathField(String pathField) {
    CdfBigQueryPropertiesActions.getSchema();
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
                            + TestSetupHooks.gcsBqTargetTable + "` ");
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
                            "WHERE table_name = '" + TestSetupHooks.gcsBqTargetTable
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

  @Then("Validate the cmek key of target GCS bucket if cmek is enabled")
  public void validateTheCmekKeyOfTargetGCSBucketIfCmekIsEnabled() throws IOException {
    if (cmekGCS != null) {
      Assert.assertEquals("Cmek key of target GCS bucket should be equal to cmek key provided in config file"
        , StorageClient.getBucketCmekKey(TestSetupHooks.gcsTargetBucketName), cmekGCS);
    } else {
      BeforeActions.scenario.write("CMEK not enabled");
    }
  }
}
