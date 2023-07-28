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

import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.apache.spark.sql.types.Decimal;
import org.joda.time.DateTime;
import org.junit.Assert;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

/**
 * BigQuery Plugin validation.
 */
public class BQValidation {

  private static List<JsonObject> getBigQueryTableData(String table)
    throws IOException, InterruptedException {
    List<JsonObject> bigQueryRows = new ArrayList<>();
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);
    result.iterateAll().forEach(value -> {
      String json = value.get(0).getStringValue();
      JsonObject jsonObject = new JsonParser().parse(json).getAsJsonObject();
      bigQueryRows.add(jsonObject);
    });

    return bigQueryRows;
  }

  public static boolean validateSourceBQToTargetBQRecord(String sourceTable, String targetTable)
    throws IOException, InterruptedException {
    List<JsonObject> bigQuerySourceResponse = getBigQueryTableData(sourceTable);
    List<JsonObject> bigQueryTargetResponse = getBigQueryTableData(targetTable);

    // Compare the data from the source and target BigQuery tables
    return compareJsonDataWithJsonData(bigQuerySourceResponse, bigQueryTargetResponse, targetTable);
  }
  public static boolean compareJsonDataWithJsonData(List<JsonObject> sourceData, List<JsonObject> targetData,
                                                    String tableName) {
    if (targetData == null) {
      Assert.fail("targetData is null");
      return false;
    }

    if (sourceData.size() != targetData.size()) {
      Assert.fail("Number of rows in source table is not equal to the number of rows in target table");
      return false;
    }

    com.google.cloud.bigquery.BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    // Build the table reference
    TableId tableRef = TableId.of(projectId, dataset, tableName);
    // Get the table schema
    Schema schema = bigQuery.getTable(tableRef).getDefinition().getSchema();

    for (int rowIndex = 0; rowIndex < sourceData.size(); rowIndex++) {
      JsonObject sourceObject = sourceData.get(rowIndex);
      JsonObject targetObject = targetData.get(rowIndex);

      for (Field field : schema.getFields()) {
        String columnName = field.getName();
        String columnTypeName = field.getType().getStandardType().toString();

        if (!sourceObject.has(columnName) || !targetObject.has(columnName)) {
          Assert.fail("Column not found in source or target data: " + columnName);
          return false;
        }

        switch (columnTypeName) {
          case "TIMESTAMP":
            Timestamp timestampSource = Timestamp.parseTimestamp(sourceObject.get(columnName).getAsString());
            Timestamp timestampTarget = Timestamp.parseTimestamp(targetObject.get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", timestampSource, timestampTarget);
            break;
          case "BOOL":
            Boolean booleanSource = Boolean.valueOf(sourceObject.get(columnName).getAsString());
            Boolean booleanTarget = Boolean.valueOf(targetObject.get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", booleanSource, booleanTarget);
            break;
          case "INTEGER":
            Integer integerSource = sourceObject.get(columnName).getAsInt();
            Integer integerTarget = targetObject.get(columnName).getAsInt();
            Assert.assertEquals("Different values found for column : %s", integerSource, integerTarget);
            break;
          case "STRING":
            String source = sourceObject.get(columnName).getAsString();
            String target = targetObject.get(columnName).getAsString();
            Assert.assertEquals("Different values found for column : %s", source, target);
            break;
          case "DATE":
            java.sql.Date dateSource = java.sql.Date.valueOf(sourceObject.get(columnName).getAsString());
            java.sql.Date dateTarget = java.sql.Date.valueOf(targetObject.get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", dateSource, dateTarget);
            break;
          case "DATETIME":
            DateTime sourceDatetime = DateTime.parse(sourceObject.get(columnName).getAsString());
            DateTime targetDateTime = DateTime.parse(targetObject.get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", sourceDatetime, targetDateTime);
            break;
          case "NUMERIC":
            Decimal decimalSource = Decimal.fromDecimal(sourceObject.get(columnName).getAsBigDecimal());
            Decimal decimalTarget = Decimal.fromDecimal(targetObject.get(columnName).getAsBigDecimal());
            Assert.assertEquals("Different values found for column : %s", decimalSource, decimalTarget);
            break;
          case "BIGNUMERIC":
            BigDecimal bigDecimalSource = sourceObject.get(columnName).getAsBigDecimal();
            BigDecimal bigDecimalTarget = targetObject.get(columnName).getAsBigDecimal();
            Assert.assertEquals("Different values found for column : %s", bigDecimalSource, bigDecimalTarget);
            break;
          case "FLOAT":
            Double sourceFloat = sourceObject.get(columnName).getAsDouble();
            Double targetFloat = targetObject.get(columnName).getAsDouble();
            Assert.assertEquals("Different values found for column : %s", sourceFloat, targetFloat);
            break;
          case "TIME":
            Time sourceTime = Time.valueOf(sourceObject.get(columnName).getAsString());
            Time targetTime = Time.valueOf(targetObject.get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", sourceTime, targetTime);
            break;

          default:
            String sourceValue = sourceObject.get(columnName).toString();
            String targetValue = targetObject.get(columnName).toString();
            Assert.assertEquals("Different values found for column : %s", sourceValue, targetValue);
            break;
        }
      }
    }
    return true;
  }
}
