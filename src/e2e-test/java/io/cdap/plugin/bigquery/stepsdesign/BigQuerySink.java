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
package io.cdap.plugin.bigquery.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.utils.E2EHelper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.time.DateUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * BigQuery Sink related stepDesigns.
 */
public class BigQuerySink implements E2EHelper {

  @When("Sink is BigQuery")
  public void sinkIsBigQuery() {
    CdfStudioActions.expandPluginGroupIfNotAlreadyExpanded("Sink");
    selectSinkPlugin("BigQueryTable");
  }

  @Then("Open BigQuery sink properties")
  public void openBigQuerySinkProperties() {
    openSinkPluginProperties("BigQuery");
  }

  @Then("Enter BigQuery sink property table name")
  public void enterBigQuerySinkPropertyTableName() {
    CdfBigQueryPropertiesActions.enterBigQueryTable(TestSetupHooks.bqTargetTable);
  }

  @Then("Toggle BigQuery sink property truncateTable to true")
  public void toggleBigQuerySinkPropertyTruncateTableToTrue() {
    CdfBigQueryPropertiesActions.clickTruncatableSwitch();
  }

  @Then("Toggle BigQuery sink property reuire partition filter to true")
  public void toggleBigQuerysinkpropertyreuirepartitionfiltertotrue() {
    CdfBigQueryPropertiesActions.toggleRequirePartitionFilter();
  }

  @Then("Toggle BigQuery sink property updateTableSchema to true")
  public void toggleBigQuerySinkPropertyUpdateTableSchemaToTrue() {

    CdfBigQueryPropertiesActions.clickUpdateTable();
  }

  @Then("Enter the BigQuery sink mandatory properties")
  public void enterTheBigQuerySinkMandatoryProperties() throws IOException {
    CdfBigQueryPropertiesActions.enterProjectId(PluginPropertyUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(PluginPropertyUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_Ref_" + UUID.randomUUID());
    CdfBigQueryPropertiesActions.enterBigQueryDataset(PluginPropertyUtils.pluginProp("dataset"));
    CdfBigQueryPropertiesActions.enterBigQueryTable(TestSetupHooks.bqTargetTable);
    CdfBigQueryPropertiesActions.clickUpdateTable();
    CdfBigQueryPropertiesActions.clickTruncatableSwitch();
  }

  @Then("Click on preview data for BigQuery sink")
  public void clickOnPreviewDataForBigQuerySink() {
    openSinkPluginPreviewData("BigQuery");
  }

  @Then("Enter BigQuery sink property {string} as macro argument {string}")
  public void enterBigQueryPropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    enterPropertyAsMacroArgument(pluginProperty, macroArgument);
  }

  @Then("Enter runtime argument value for BigQuery sink table name key {string}")
  public void enterRuntimeArgumentValueForBigQuerySinkTableNameKey(String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey), TestSetupHooks.bqTargetTable);
  }

  @Then("Select BigQuery sink property partitioning type as {string}")
  public void selectBigQuerySinkPropertyPartitioningTypeAs(String partitioningType) {
    CdfBigQueryPropertiesActions.selectPartitioningType(partitioningType);
  }

  @Then("Enter BigQuery sink property partition field {string}")
  public void enterBigQuerySinkPropertyPartitionField(String partitionField) {
    CdfBigQueryPropertiesActions.enterPartitionField(PluginPropertyUtils.pluginProp(partitionField));
  }

  @Then("Enter BigQuery sink property range start {string}")
  public void enterBigQuerySinkPropertyRangeStart(String rangeStart) {
    CdfBigQueryPropertiesActions.enterRangeStart(PluginPropertyUtils.pluginProp(rangeStart));
  }

  @Then("Enter BigQuery sink property range end {string}")
  public void enterBigQuerySinkPropertyRangeEnd(String rangeEnd) {
    CdfBigQueryPropertiesActions.enterRangeEnd(PluginPropertyUtils.pluginProp(rangeEnd));
  }

  @Then("Enter BigQuery sink property range interval {string}")
  public void enterBigQuerySinkPropertyRangeInterval(String rangeInterval) {
    CdfBigQueryPropertiesActions.enterRangeInterval(PluginPropertyUtils.pluginProp(rangeInterval));
  }

  @Then("Toggle BigQuery sink property require partition filter to true")
  public void toggleBigQuerySinkPropertyRequirePartitionFilterToTrue() {
    CdfBigQueryPropertiesActions.toggleRequirePartitionFilter();
  }

  @Then("Enter BigQuery sink property GCS upload request chunk size {string}")
  public void enterBigQuerySinkPropertyGCSUploadRequestChunkSize(String chunkSize) {
    CdfBigQueryPropertiesActions.enterChunkSize(PluginPropertyUtils.pluginProp(chunkSize));
  }
}
