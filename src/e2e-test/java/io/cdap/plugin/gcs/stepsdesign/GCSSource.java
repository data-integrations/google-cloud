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

import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.locators.CdfGCSLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.gcs.GCSValidationHelper;
import io.cdap.plugin.utils.CdfPluginPropertyLocator;
import io.cdap.plugin.utils.E2EHelper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;

import java.io.IOException;

/**
 * GCS Source Plugin related step design.
 */
public class GCSSource implements E2EHelper {

  @When("Source is GCS")
  public void sourceIsGCS() {
    selectSourcePlugin("GCSFile");
  }

  @Then("Open GCS source properties")
  public void openGCSSourceProperties() {
    openSourcePluginProperties("GCS");
  }

  @Then("Enter GCS source property path {string}")
  public void enterGCSSourcePropertyPath(String path) {
    CdfGcsActions.getGcsBucket("gs://" + TestSetupHooks.gcsSourceBucketName + "/"
                                 + PluginPropertyUtils.pluginProp(path));
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

  @Then("Enter GCS source property minimum split size {string} and maximum split size {string}")
  public void enterGCSSourcePropertyMinimumSplitSizeAndMaximumSplitSize(String minSplitSize, String maxSplitSize) {
    CdfGcsActions.enterMaxSplitSize(PluginPropertyUtils.pluginProp(minSplitSize));
    CdfGcsActions.enterMinSplitSize(PluginPropertyUtils.pluginProp(maxSplitSize));
  }

  @Then("Enter GCS source property regex path filter {string}")
  public void enterGCSSourcePropertyRegexPathFilter(String regexPathFilter) {
    CdfGcsActions.enterRegexPath(PluginPropertyUtils.pluginProp(regexPathFilter));
  }

  @Then("Enter the GCS source mandatory properties")
  public void enterTheGCSSourceMandatoryProperties() throws InterruptedException, IOException {
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.getGcsBucket("gs://" + TestSetupHooks.gcsSourceBucketName + "/"
                                 + PluginPropertyUtils.pluginProp("gcsCsvFile"));
    CdfGcsActions.selectFormat("csv");
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfGCSLocators.getSchemaLoadComplete, 20L);
  }

  @Then("Enter GCS source property {string} as macro argument {string}")
  public void enterGCSSourcePropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    enterPropertyAsMacroArgument(pluginProperty, macroArgument);
  }

  @Then("Enter GCS source property output schema {string} as macro argument {string}")
  public void enterGCSSourcePropertyOutputSchemaAsMacroArgument(String pluginProperty, String macroArgument) {
    SCHEMA_LOCATORS.schemaActions.click();
    SCHEMA_LOCATORS.schemaActionType("macro").click();
    WaitHelper.waitForElementToBeHidden(SCHEMA_LOCATORS.schemaActionType("macro"), 5);
    try {
      enterMacro(CdfPluginPropertyLocator.fromPropertyString(pluginProperty).pluginProperty, macroArgument);
    } catch (NullPointerException e) {
      Assert.fail("CDF_PLUGIN_PROPERTY_MAPPING for '" + pluginProperty + "' not present in CdfPluginPropertyLocator.");
    }
  }

  @Then("Enter runtime argument value {string} for GCS source property path key {string}")
  public void enterRuntimeArgumentValueForGCSSourcePropertyPathKey(String value, String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey),
                           "gs://" + TestSetupHooks.gcsSourceBucketName + "/" + PluginPropertyUtils.pluginProp(value));
  }

  @Then("Select GCS source property file encoding type {string}")
  public void selectGCSSourcePropertyFileEncodingType(String encoding) {
    CdfGcsActions.selectFileEncoding(encoding);
  }

  @Then("Select GCS source property read file recursive as {string}")
  public void selectGCSSourcePropertyReadFileRecursiveAs(String value) {
    CdfGcsActions.selectRecursive(value);
  }

  @Then("Select GCS source property path filename only as {string}")
  public void selectGCSSourcePropertyPathFilenameOnlyAs(String value) {
    CdfGcsActions.selectPathFilenameOnly(value);
  }

  @Then("Validate the data transferred from GCS Source to GCS Sink with Expected avro file and target data in " +
    "GCS bucket")
  public void validateTheDataTransferredFromGCSSourceToGCSSinkWithExpectedAvroFileAndTargetDataInGCSBucket() throws
    IOException {
    GCSValidationHelper.validateGCSSourceToGCSSinkWithAVROFormat(TestSetupHooks.gcsTargetBucketName);
  }

  @Then("Validate the data from GCS Source to GCS Sink with expected csv file and target data in GCS bucket")
  public void validateTheDataFromGCSSourceToGCSSinkWithExpectedCsvFileAndTargetDataInGCSBucket() {
    GCSValidationHelper.validateGCSSourceToGCSSinkWithCSVFormat(TestSetupHooks.gcsTargetBucketName);
  }
  @Then("Validate the data from GCS Source to GCS Sink with expected json file and target data in GCS bucket")
  public void validateTheDataFromGCSSourceToGCSSinkWithExpectedJsonFileAndTargetDataInGCSBucket() {
    GCSValidationHelper.validateGCSSourceToGCSSinkWithJsonFormat(TestSetupHooks.gcsTargetBucketName);
  }
}
