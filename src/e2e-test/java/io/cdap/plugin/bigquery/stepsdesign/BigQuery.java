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

package io.cdap.plugin.bigquery.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * BigQuery Plugin validation common step design.
 */
public class BigQuery {

  @Then("Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table")
  public void validateTheValuesOfRecordsTransferredToBQsinkIsEqualToTheValuesFromSourceBigQueryTable()
    throws InterruptedException, IOException {
    int sourceBQRecordsCount = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp("bqSourceTable"));
    BeforeActions.scenario.write("No of Records from source BigQuery table:" + sourceBQRecordsCount);
    Assert.assertEquals("Out records should match with BigQuery source records count",
                        CdfPipelineRunAction.getCountDisplayedOnSourcePluginAsRecordsOut(), sourceBQRecordsCount);

    boolean recordsMatched = BQValidation.validateSourceBQToTargetBQRecord(
      TestSetupHooks.bqSourceTable, TestSetupHooks.bqTargetTable);
    Assert.assertTrue("Value of records transferred to the BQ sink should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }

  @Then("Validate The Data From BQ To BQ With Actual And Expected File for: {string}")
  public void validateTheDataFromBQToBQWithActualAndExpectedFileFor(String expectedFile) throws IOException,
    InterruptedException, URISyntaxException {
    boolean recordsMatched = ValidationHelperSqlEngine.validateActualDataToExpectedData(
      PluginPropertyUtils.pluginProp("bqTargetTable"),
      PluginPropertyUtils.pluginProp(expectedFile));
    Assert.assertTrue("Value of records in actual and expected file is equal", recordsMatched);
  }
}
