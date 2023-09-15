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
import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.utils.E2EHelper;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Assert;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * BigQuery Source related stepDesigns.
 */
public class BigQuerySource implements E2EHelper {

  @When("Source is BigQuery")
  public void sourceIsBigQuery() {
    selectSourcePlugin("BigQueryTable");
  }

  @Then("Open BigQuery source properties")
  public void openBigQuerySourceProperties() {
    openSourcePluginProperties("BigQuery");
  }

  @Then("Enter BigQuery source property table name")
  public void enterBigQuerySourcePropertyTableName() {
    CdfBigQueryPropertiesActions.enterBigQueryTable(TestSetupHooks.bqSourceTable);
  }

  @Then("Enter BigQuery source property filter {string}")
  public void enterBigQuerySourcePropertyFilter(String filter) throws IOException {
    CdfBigQueryPropertiesActions.enterFilter(PluginPropertyUtils.pluginProp(filter));
  }

  @Then("Enter BigQuery source properties partitionStartDate and partitionEndDate")
  public void enterBigQuerySourcePropertiesPartitionStartDateAndPartitionEndDate() throws IOException {
    CdfBigQueryPropertiesActions.enterPartitionStartDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
    CdfBigQueryPropertiesActions.enterPartitionEndDate(new SimpleDateFormat("yyyy-MM-dd")
                                                         .format(DateUtils.addDays(new Date(), 1)));
  }

  @Then("Enter BigQuery source properties partitionStartDate {string} and partitionEndDate {string}")
  public void enterBigQuerySourcePropertiesPartitionStartDateAndPartitionEndDate(
    String partitionStartDate, String partitionEndDate) throws IOException {
    CdfBigQueryPropertiesActions.enterPartitionStartDate(PluginPropertyUtils.pluginProp(partitionStartDate));
    CdfBigQueryPropertiesActions.enterPartitionEndDate(PluginPropertyUtils.pluginProp(partitionEndDate));
  }

  @Then("Enter the BigQuery source mandatory properties")
  public void enterTheBigQuerySourceMandatoryProperties() throws IOException {
    CdfBigQueryPropertiesActions.enterProjectId(PluginPropertyUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(PluginPropertyUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName("BQ_Ref_" + UUID.randomUUID());
    CdfBigQueryPropertiesActions.enterBigQueryDataset(PluginPropertyUtils.pluginProp("dataset"));
    CdfBigQueryPropertiesActions.enterBigQueryTable(TestSetupHooks.bqSourceTable);
  }

  @Then("Enter the BigQuery source properties with incorrect property {string} value {string}")
  public void enterTheBigQuerySourcePropertiesWithIncorrectProperty(String property, String value) throws IOException {
    CdfBigQueryPropertiesActions.enterProjectId(PluginPropertyUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterDatasetProjectId(PluginPropertyUtils.pluginProp("projectId"));
    CdfBigQueryPropertiesActions.enterBigQueryDataset(PluginPropertyUtils.pluginProp("dataset"));
    CdfBigQueryPropertiesActions.enterBigQueryTable(TestSetupHooks.bqSourceTable);
    if (property.equalsIgnoreCase("dataset")) {
      SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.bigQueryDataSet,
                                         PluginPropertyUtils.pluginProp(value));
    } else if (property.equalsIgnoreCase("table")) {
      SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.bigQueryTable,
                                         PluginPropertyUtils.pluginProp(value));
    } else if (property.equalsIgnoreCase("datasetProject")) {
      SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.datasetProjectID,
                                         PluginPropertyUtils.pluginProp(value));
    } else {
      Assert.fail("Invalid BigQuery property " + property);
    }
  }

  @Then("Validate BigQuery source incorrect property error for table {string} value {string}")
  public void validateBigQuerySourceIncorrectPropertyErrorFor(String property, String value) {
    CdfBigQueryPropertiesActions.getSchema();
    SeleniumHelper.waitElementIsVisible(CdfBigQueryPropertiesLocators.getSchemaButton, 5L);
    String tableFullName = StringUtils.EMPTY;
    if (property.equalsIgnoreCase("dataset")) {
      tableFullName = PluginPropertyUtils.pluginProp("projectId") + ":" + PluginPropertyUtils.pluginProp(value)
        + "." + TestSetupHooks.bqSourceTable;
    } else if (property.equalsIgnoreCase("table")) {
      tableFullName = PluginPropertyUtils.pluginProp("projectId") + ":"
        + PluginPropertyUtils.pluginProp("dataset")
        + "." + PluginPropertyUtils.pluginProp(value);
    } else if (property.equalsIgnoreCase("datasetProject")) {
      tableFullName = PluginPropertyUtils.pluginProp(value) + ":" + PluginPropertyUtils.pluginProp("dataset")
        + "." + TestSetupHooks.bqSourceTable;
    }
    String expectedErrorMessage = PluginPropertyUtils.errorProp(E2ETestConstants.ERROR_MSG_INCORRECT_TABLE)
      .replaceAll("TABLENAME", tableFullName);
    String actualErrorMessage = PluginPropertyUtils.findPropertyErrorElement("table").getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = PluginPropertyUtils.getErrorColor(PluginPropertyUtils.findPropertyErrorElement("table"));
    String expectedColor = ConstantsUtil.ERROR_MSG_COLOR;
    Assert.assertEquals(expectedColor, actualColor);
  }

  @Then("Enter BigQuery source property {string} as macro argument {string}")
  public void enterBigQuerySourcePropertyAsMacroArgument(String pluginProperty, String macroArgument) {
    enterPropertyAsMacroArgument(pluginProperty, macroArgument);
  }

  @Then("Enter runtime argument value for BigQuery source table name key {string}")
  public void enterRuntimeArgumentValueForBigQuerySourceTableNameKey(String runtimeArgumentKey) {
    ElementHelper.sendKeys(CdfStudioLocators.runtimeArgsValue(runtimeArgumentKey), TestSetupHooks.bqSourceTable);
  }

  @Then("Toggle BigQuery source property enable querying views to true")
  public void toggleBigQuerySourcePropertyEnableQueryingViewsToTrue() {
    CdfBigQueryPropertiesActions.toggleEnableQueryingViews();
  }

  @Then("Enter the BigQuery source property for view materialization project {string}")
  public void enterTheBigQuerySourcePropertyForViewMaterializationProject(String viewMaterializationProject) {
    CdfBigQueryPropertiesActions.enterViewMaterializationProject(PluginPropertyUtils.
      pluginProp(viewMaterializationProject));
  }

  @Then("Enter the BigQuery source property for view materialization dataset {string}")
  public void enterTheBigQuerySourcePropertyForViewMaterializationDataset(String viewMaterializationDataset) {
    CdfBigQueryPropertiesActions.enterViewMaterializationDataset(PluginPropertyUtils.
      pluginProp(viewMaterializationDataset));
  }

  @Then("Enter BigQuery source property table name as view")
  public void enterBigQuerySourcePropertyTableNameAsView() {
    CdfBigQueryPropertiesActions.enterBigQueryTable(TestSetupHooks.bqSourceView);
  }

  @Then("Validate the data transferred from BigQuery to BigQuery with actual And expected file for: {string}")
  public void validateTheDataFromBQToBQWithActualAndExpectedFileFor(String expectedFile) throws IOException,
    InterruptedException, URISyntaxException {
    boolean recordsMatched = BQValidationExistingTables.validateActualDataToExpectedData(
      PluginPropertyUtils.pluginProp("bqTargetTable"),
      PluginPropertyUtils.pluginProp(expectedFile));
    Assert.assertTrue("Value of records in actual and expected file is equal", recordsMatched);
  }
}
