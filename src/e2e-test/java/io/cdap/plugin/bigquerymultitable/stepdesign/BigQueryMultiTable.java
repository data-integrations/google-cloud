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

package io.cdap.plugin.bigquerymultitable.stepdesign;

import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cucumber.java.en.Then;
import org.junit.Assert;

import java.io.IOException;
import java.sql.SQLException;

/**
 * BigQueryMultiTable Plugin validation common step design.
 */
public class BigQueryMultiTable {

  @Then("Validate the values of records transferred to BQMT sink is equal to the value from source MultiDatabase table")
  public void validateTheValuesOfRecordsTransferredToBQMTsinkIsEqualToTheValuesFromSourceMultiDatabaseTable()
    throws InterruptedException, IOException, SQLException, ClassNotFoundException {
    boolean recordsMatched = BQMultiTableValidation.validateMySqlToBQRecordValues(
      PluginPropertyUtils.pluginProp("sourceTable"));
    Assert.assertTrue("Value of records transferred to the BQ sink should be equal to the value " +
                        "of the records in the source table", recordsMatched);

  }

  }

