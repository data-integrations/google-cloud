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

import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * BQValidation.
 */
public class BigQueryMultiTableValidation {
  static Gson gson = new Gson();
  public static boolean validateBQToBigQueryMultiTable(List<String> sourceTables, List<String> targetTables)
    throws IOException, InterruptedException {
    // Check if there's only one source table
    if (sourceTables.size() == 1) {
      String currentTargetTable = targetTables.get(0);
      return validateBQToBQSingleTable(sourceTables.get(0), currentTargetTable);
    } else {
      if (sourceTables.size() != targetTables.size()) {
        throw new IllegalArgumentException("Number of source tables and target tables must be the same.");
      }
      // Iterate over each pair of source and target tables
      for (int i = 0; i < sourceTables.size(); i++) {
        String currentSourceTable = sourceTables.get(i);
        String currentTargetTable = targetTables.get(i);
        // Perform validation for the current pair of tables
        if (!validateBQToBQSingleTable(currentSourceTable, currentTargetTable)) {
          return false; // Return false if validation fails for any table pair
        }
      }
      // Return true if validation passes for all table pairs
      return true;
    }
  }

  public static boolean validateBQToBQSingleTable(String sourceTable, String targetTable)
    throws IOException, InterruptedException {
    // Fetch data from the source BigQuery table
    List<Object> bigQueryRowsSource = new ArrayList<>();
    getBigQueryTableData(sourceTable, bigQueryRowsSource);

    // Convert fetched data into JSON objects
    List<JsonObject> bigQuerySourceResponse = new ArrayList<>();
    for (Object row : bigQueryRowsSource) {
      JsonObject jsonData = gson.fromJson(String.valueOf(row), JsonObject.class);
      bigQuerySourceResponse.add(jsonData);
    }
    // Fetch data from the target BigQuery table
    List<Object> bigQueryRowsTarget = new ArrayList<>();
    getBigQueryTableData(targetTable, bigQueryRowsTarget);
    List<JsonObject> bigQueryTargetResponse = new ArrayList<>();
    for (Object row : bigQueryRowsTarget) {
      JsonObject jsonData = gson.fromJson(String.valueOf(row), JsonObject.class);
      bigQueryTargetResponse.add(jsonData);
    }
    return compareBigQueryDataAndBQMT(bigQuerySourceResponse, bigQueryTargetResponse);
  }

  private static void getBigQueryTableData(String table, List<Object> bigQueryRows) throws IOException,
    InterruptedException {
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);
    result.iterateAll().forEach(value -> bigQueryRows.add(value.get(0).getValue()));
  }

  private static boolean compareBigQueryDataAndBQMT(List<JsonObject> bigQueryResponse, List<JsonObject> bqmtData)
    throws NullPointerException {
    if (bigQueryResponse.size() != bqmtData.size()) {
      return false;
    }
    // Compare individual elements
    for (int i = 0; i < bigQueryResponse.size(); i++) {
      JsonObject obj1 = bigQueryResponse.get(i);
      JsonObject obj2 = bqmtData.get(i);
      if (!obj1.equals(obj2)) {
        return false;
      }
    }
    return true;
  }
}
