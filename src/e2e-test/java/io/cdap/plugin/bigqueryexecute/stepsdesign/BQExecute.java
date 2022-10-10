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
package io.cdap.plugin.bigqueryexecute.stepsdesign;

import com.google.cloud.bigquery.BigQueryException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.util.Optional;

/**
 * BigQuery Execute Connector related Step Design.
 */
public class BQExecute {
  @Then("Validate records transferred to target BigQuery Table: {string} is equal to number of records fetched from " +
    "SQL query: {string}")
  public void validateRecordsTransferredToTargetBigQueryTableIsEqualToNumberOfRecordsFetchedFromSQLQuery(
    String targetTable, String query)
    throws IOException, InterruptedException {
    int countRecordsTarget = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp(targetTable));
    Optional<String> result = BigQueryClient.getSoleQueryResult("Select count(*) from ( "
                                                                  + PluginPropertyUtils.pluginProp(query) + " )");
    int count = result.map(Integer::parseInt).orElse(0);
    BeforeActions.scenario.write("Number of records fetched with sql query :" + count);
    Assert.assertEquals(count, countRecordsTarget);
  }

  @Then("Verify BigQuery table: {string} is created")
  public void verifyBigQueryTableIsCreated(String table) throws IOException, InterruptedException {
    try {
      BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp(table));
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ table " + PluginPropertyUtils.pluginProp(table) + " is not created");
        Assert.fail("BQ table " + PluginPropertyUtils.pluginProp(table) + " is not created");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Then("Verify BigQuery table: {string} is deleted")
  public void verifyBigQueryTableIsDeleted(String table) throws IOException, InterruptedException {
    try {
      BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp(table));
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ table " + PluginPropertyUtils.pluginProp(table) + " is deleted");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Then("Verify {int} records inserted in BigQuery table: {string} with query {string}")
  public void verifyNumberOfRecordsInsertedInBigQueryTableWithQuery(int expectedCount, String table, String countQuery)
    throws IOException, InterruptedException {
    int count = TestSetupHooks.getBigQueryRecordsCountByQuery(table, countQuery);
    Assert.assertEquals("BQExecute plugin should insert record in BigQuery table", expectedCount, count);
  }

  @Then("Verify {int} records updated in BigQuery table: {string} with query {string}")
  public void verifyRecordsUpdatedInBigQueryTableWithQuery(int expectedCount, String table, String countQuery)
    throws IOException, InterruptedException {
    int count = TestSetupHooks.getBigQueryRecordsCountByQuery(table, countQuery);
    Assert.assertEquals("BQExecute plugin should update record in BigQuery table", expectedCount, count);
  }
}
