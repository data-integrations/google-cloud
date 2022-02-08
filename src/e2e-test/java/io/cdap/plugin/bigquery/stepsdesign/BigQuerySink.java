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
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.io.IOException;
import java.util.UUID;

/**
 * BigQuery Sink related stepDesigns.
 */
public class BigQuerySink implements CdfHelper {

  @When("Sink is BigQuery")
  public void sinkIsBigQuery() {
    CdfStudioActions.clickSink();
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

}
