/*
 * Copyright © 2024 Cask Data, Inc.
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

import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cucumber.java.en.Then;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * BigQuery MultiTable related common stepDesigns.
 */
public class BigQueryMultiTable {
  @Then("Validate data transferred from BigQuery To BigQueryMultiTable is equal")
  public void validateDataTransferredFromBigQueryToBigQueryMultiTableIsEqual()
    throws IOException, InterruptedException {
    List<String> sourceTables = Arrays.asList(PluginPropertyUtils.pluginProp("bqSourceTable"),
                                        PluginPropertyUtils.pluginProp("bqSourceTable2"));

    List<String> targetTables = Arrays.asList(PluginPropertyUtils.pluginProp("bqTargetTable"),
                                              PluginPropertyUtils.pluginProp("bqTargetTable2"));
    boolean recordsMatched = BigQueryMultiTableValidation.validateBQToBigQueryMultiTable(sourceTables, targetTables);
    Assert.assertTrue("Value of records transferred to the BQ sink should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }

  @Then("Validate data transferred from BigQuery To BigQueryMultiTable in one table is equal")
  public void validateDataTransferredFromBigQueryToBigQueryMultiTableInOneTableIsEqual()
    throws IOException, InterruptedException {
    boolean recordsMatched = BigQueryMultiTableValidation.
      validateBQToBigQueryMultiTable(Collections.singletonList(PluginPropertyUtils.pluginProp("bqSourceTable")),
                                     Collections.singletonList(PluginPropertyUtils.pluginProp("bqTargetTable")));
    Assert.assertTrue("Value of records transferred to the BQ sink should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }
}
