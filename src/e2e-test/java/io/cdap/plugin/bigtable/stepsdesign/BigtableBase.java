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

package io.cdap.plugin.bigtable.stepsdesign;

import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.utils.BigTableClient;
import io.cucumber.java.en.Then;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.io.IOException;

/**
 * BigTable plugin related common test step definitions.
 */

public class BigtableBase {
    @Then("Validate data transferred to target bigtable table with data of source bigtable table")
    public void validateDataTransferToTargetBigtableTable() throws IOException {
        Table sinkTable = BigTableClient.getTable(TestSetupHooks.bigTableConnection,
                PluginPropertyUtils.pluginProp("bigtableTargetTable"));
        validateData(sinkTable);
    }
    @Then("Validate data transferred to existing target bigtable table with data of source bigtable table")
    public void validateDataTransferToTargetExistingBigtableTable() throws IOException {
        Table existingSinkTable = BigTableClient.getTable(TestSetupHooks.bigTableExistingTargetTableConnection,
                PluginPropertyUtils.pluginProp("bigtableTargetExistingTable"));
        validateData(existingSinkTable);
    }
    public static void validateData(Table tableToBeValidated) throws IOException {
        Result result = tableToBeValidated.get(new Get(Bytes.toBytes("r1")));
        Assert.assertTrue(Bytes.toBoolean(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("boolean_column"))));
        Assert.assertEquals("bytes",
                Bytes.toString(result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("bytes_column"))));
        Assert.assertEquals(10.5D,
                Bytes.toDouble(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("double_column"))),
                0.0000001);
        Assert.assertEquals(10.5F,
                Bytes.toFloat(result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("float_column"))),
                0.0000001);
        Assert.assertEquals(1,
                Bytes.toInt(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("int_column"))));
        Assert.assertEquals(10L,
                Bytes.toLong(result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("long_column"))));
        Assert.assertEquals("string",
                Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("string_column"))));

    }
}

