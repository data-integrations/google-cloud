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
package io.cdap.plugin.spanner.stepsdesign;

import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.spanner.actions.SpannerActions;
import io.cdap.plugin.spanner.locators.SpannerLocators;
import io.cdap.plugin.utils.SpannerClient;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.util.Optional;

/**
 * Spanner plugin related common test step definitions.
 */
public class SpannerBase implements CdfHelper {

  @Then("Enter Spanner property reference name")
  public void enterSpannerPropertyReferenceName() {
    SpannerActions.enterReferenceName();
  }

  @Then("Enter Spanner property projectId {string}")
  public void enterSpannerPropertyProjectId(String projectId) {
    SpannerActions.enterProjectId(PluginPropertyUtils.pluginProp(projectId));
  }

  @Then("Enter Spanner property InstanceId")
  public void enterSpannerPropertyInstanceId() {
    SpannerActions.enterInstanceID(TestSetupHooks.spannerInstance);
  }

  @Then("Close the Spanner properties")
  public void closeTheSpannerProperties() {
    SpannerActions.clickCloseButton();
  }

  @Then("Validate records transferred to target BigQuery table with record counts of spanner table")
  public void validateRecordsTransferredToTargetBigQueryTableWithRecordCountsOfSpannerTable()
    throws IOException, InterruptedException {
    int spannerTableRecordCount = SpannerClient
      .getCountOfRecordsInTable(TestSetupHooks.spannerInstance, TestSetupHooks.spannerDatabase,
                                TestSetupHooks.spannerSourceTable);
    BeforeActions.scenario.write("No of records from Spanner table :" + spannerTableRecordCount);
    int bqTargetRecordCount = BigQueryClient.countBqQuery(TestSetupHooks.bqTargetTable);
    BeforeActions.scenario.write("No of Records transferred to BigQuery :" + bqTargetRecordCount);
    Assert.assertEquals(spannerTableRecordCount, bqTargetRecordCount);
  }

  @Then("Validate records transferred to target BigQuery table with record counts of spanner Import Query {string}")
  public void validateRecordsTransferredToTargetBigQueryTableWithRecordCountsOfSpannerImportQuery(String query)
    throws IOException, InterruptedException {
    Optional<String> result = SpannerClient.
      getSoleQueryResult(TestSetupHooks.spannerInstance, TestSetupHooks.spannerDatabase,
                         PluginPropertyUtils.pluginProp(query));
    int spannerQueryRecordCount = 0;
    if (result.isPresent()) {
      spannerQueryRecordCount = Integer.parseInt(result.get());
    }
    BeforeActions.scenario.write("No of Records from Spanner Query :" + spannerQueryRecordCount);
    int bqTargetRecordCount = BigQueryClient.countBqQuery(TestSetupHooks.bqTargetTable);
    BeforeActions.scenario.write("No of Records transferred to BigQuery :" + bqTargetRecordCount);
    Assert.assertEquals(spannerQueryRecordCount, bqTargetRecordCount);
  }

  @Then("Validate records transferred to target spanner table with record counts of BigQuery table")
  public void validateRecordsTransferredToTargetSpannerTableWithRecordCountsOfBigQueryTable()
    throws IOException, InterruptedException {
    int bqSourceRecordCount = BigQueryClient.countBqQuery(TestSetupHooks.bqSourceTable);
    BeforeActions.scenario.write("No of Records from source BigQuery table:" + bqSourceRecordCount);
    int spannerTargetTableRecordCount = SpannerClient
      .getCountOfRecordsInTable(TestSetupHooks.spannerInstance, TestSetupHooks.spannerTargetDatabase,
                                TestSetupHooks.spannerTargetTable);
    BeforeActions.scenario.write("No of records transferred to Spanner table:" + spannerTargetTableRecordCount);

    Assert.assertEquals(bqSourceRecordCount, spannerTargetTableRecordCount);
  }

  @Then("Enter the Spanner properties with blank property {string}")
  public void enterTheSpannerPropertiesWithBlankProperty(String property) {
    if (!property.equalsIgnoreCase("referenceName")) {
      SpannerActions.enterReferenceName();
    }
    if (!property.equalsIgnoreCase("instance")) {
      SpannerActions.enterInstanceID(TestSetupHooks.spannerInstance);
    }
    if (!property.equalsIgnoreCase("database")) {
      SpannerActions.enterDatabaseName(TestSetupHooks.spannerDatabase);
    }
    if (!property.equalsIgnoreCase("table")) {
      SpannerActions.enterTableName(TestSetupHooks.spannerSourceTable);
    }
    if (!PluginPropertyUtils.pluginProp("spannerMandatoryProperties").contains(property)) {
      Assert.fail("Invalid Spanner mandatory property " + property);
    }
  }

  @Then("Enter the Spanner properties with incorrect property {string}")
  public void enterTheSpannerPropertiesWithIncorrectProperty(String property) {
    SpannerActions.enterInstanceID(TestSetupHooks.spannerInstance);
    SpannerActions.enterDatabaseName(TestSetupHooks.spannerDatabase);
    SpannerActions.enterTableName(TestSetupHooks.spannerSourceTable);
    if (property.equalsIgnoreCase("instance")) {
      ElementHelper.replaceElementValue(SpannerLocators.spannerInstanceId
        , PluginPropertyUtils.pluginProp("spannerIncorrectInstanceId"));
    } else if (property.equalsIgnoreCase("database")) {
      ElementHelper.replaceElementValue(SpannerLocators.spannerDatabaseName
        , PluginPropertyUtils.pluginProp("spannerIncorrectDatabaseName"));
    } else if (property.equalsIgnoreCase("table")) {
      ElementHelper.replaceElementValue(SpannerLocators.spannerTableName
        , PluginPropertyUtils.pluginProp("spannerIncorrectTableName"));
    } else if (property.equalsIgnoreCase("importQuery")) {
      SpannerActions.enterImportQuery(PluginPropertyUtils.pluginProp("spannerIncorrectQuery"));
    } else {
      Assert.fail("Invalid Spanner Advanced Property : " + property);
    }
  }
}
