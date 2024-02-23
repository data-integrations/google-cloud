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

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.junit.Assert;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * BQMTMultiTable Validation.
 */

public class BQMultiTableValidation {

  private static final String tabA = "tabA";

  private static final String tabB = "tabB";

  /**
   * Validates data from a MySQL table against corresponding BigQuery tables.
   *
   * @param sourceTable The name of the MySQL table to be validated.
   * @return true if validation passes for all tables, false otherwise.
   * @throws IOException If an I/O error occurs.
   * @throws InterruptedException If the current thread is interrupted while waiting.
   * @throws SQLException If an SQL error occurs.
   * @throws ClassNotFoundException If the class specified is not found.
   */

  public static boolean validateMySqlToBQRecordValues(String sourceTable) throws IOException, InterruptedException,
    SQLException, ClassNotFoundException {
    // using MySql database and tables in Multiple Database table plugin
    List<String> targetTables = getTableNameFromMySQL();
    List<String> sourceList = new ArrayList<>();
    sourceList.add(sourceTable);
    List<Object> bigQueryRows = new ArrayList<>();
    List<JsonObject> bigQueryResponse = new ArrayList<>();

    Gson gson = new Gson();
    for (int objNameIndex = 0; objNameIndex <= sourceList.size(); objNameIndex++) {
      String currentTargetTable = targetTables.get(objNameIndex);
      getBigQueryTableData(currentTargetTable, bigQueryRows);
    }
    for (Object row : bigQueryRows) {
      JsonObject jsonData = gson.fromJson(String.valueOf(row), JsonObject.class);
      bigQueryResponse.add(jsonData);
    }
    bigQueryResponse.sort(Comparator.comparing(jsonData -> jsonData.get("EmployeeID").getAsInt()));


    String getTargetQuery = "SELECT * FROM " + sourceTable;
    try (Connection connect = MultiTableClient.getMysqlConnection()) {
      connect.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
      Statement statement1 = connect.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                                                     ResultSet.CONCUR_UPDATABLE);

      ResultSet rsTarget = statement1.executeQuery(getTargetQuery);
      boolean isValid = compareResultSetDataToBQMultiTable(rsTarget, bigQueryResponse);

      if (!isValid) {
        return false;

      }
    }
    return true;
  }

  /**
   * Compares the data from a ResultSet with a list of JSONObjects representing BigQuery data.
   *
   * @param rsSource The ResultSet containing the data from the source table.
   * @param bigQueryData The list of JSONObjects representing the data from BigQuery.
   * @return true if the data matches between the source and BigQuery, false otherwise.
   * @throws SQLException If an SQL error occurs.
   */

  private static boolean compareResultSetDataToBQMultiTable(ResultSet rsSource, List<JsonObject>
    bigQueryData) throws SQLException {

    ResultSetMetaData mdSource = rsSource.getMetaData();
    boolean result = false;
    int columnCountSource = mdSource.getColumnCount();

    if (bigQueryData == null) {
      Assert.fail("bigQueryData is null");
      return result;
    }

    // Get the column count of the first JsonObject in bigQueryData
    int columnCountTarget = 0;
    if (bigQueryData.size() > 0) {
      columnCountTarget = bigQueryData.get(0).entrySet().size();
    }
    // Compare the number of columns in the source and target
    Assert.assertEquals("Number of columns in source and target are not equal",
                        columnCountSource, columnCountTarget);

    //Variable 'jsonObjectIdx' to track the index of the current JsonObject in the bigQueryData list,
    int jsonObjectIdx = 0;
    while (rsSource.next()) {
      int currentColumnCount = 1;
      while (currentColumnCount <= columnCountSource) {
        String columnTypeName = mdSource.getColumnTypeName(currentColumnCount);
        int columnType = mdSource.getColumnType(currentColumnCount);
        String columnName = mdSource.getColumnName(currentColumnCount);

        switch (columnType) {

          case Types.INTEGER:
            Integer sourceInteger = rsSource.getInt(currentColumnCount);
            Integer targetInteger = bigQueryData.get(jsonObjectIdx).get(columnName).getAsInt();
            Assert.assertEquals("Different values found for column : %s", sourceInteger, targetInteger);
            break;
          case Types.VARCHAR:
            String sourceString = rsSource.getString(currentColumnCount);
            String targetString = bigQueryData.get(jsonObjectIdx).get(columnName).getAsString();
            Assert.assertEquals("Different values found for column : %s", sourceString, targetString);
            break;

        }
        currentColumnCount++;
      }
      jsonObjectIdx++;
    }
    return true;
  }

  /**
   * Retrieves the data from a specified BigQuery table and populates it into the provided list of objects.
   *
   * @param table        The name of the BigQuery table to fetch data from.
   * @param bigQueryRows The list to store the fetched BigQuery data.
   */

  private static void getBigQueryTableData(String table, List<Object> bigQueryRows)
    throws IOException, InterruptedException {

    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);
    result.iterateAll().forEach(value -> bigQueryRows.add(value.get(0).getValue()));
  }

  /**
   * Fetches the list of table names from the configured dataset in BigQuery.
   *
   * @return A TableResult object containing the table names.
   * @throws IOException If an I/O error occurs.
   * @throws InterruptedException If the current thread is interrupted while waiting.
   */

  public static TableResult getTableNamesFromBQDataSet() throws IOException, InterruptedException {
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT table_name FROM `" + projectId + "." + dataset + "`.INFORMATION_SCHEMA.TABLES ";

    return BigQueryClient.getQueryResult(selectQuery);
  }

  /**
   * Fetches the names of target tables from the dataset and returns the list of matching table names.
   *
   * @return A list of target table names from the dataset.
   * @throws IOException If an I/O error occurs.
   * @throws InterruptedException If the current thread is interrupted while waiting.
   */

  public static List<String> getTableNameFromMySQL() throws IOException, InterruptedException {
    List<String> tableNames = new ArrayList<>();
    List<String> targetTableNames = Arrays.asList(tabA, tabB);
    TableResult tableResult = getTableNamesFromBQDataSet();
    Iterable<FieldValueList> rows = tableResult.iterateAll();

    for (FieldValueList row : rows) {
      FieldValue fieldValue = row.get(0);
      String currentTableName = fieldValue.getStringValue();

      if (targetTableNames.contains(currentTableName)) {
        tableNames.add(currentTableName);
      }
    }
    if (tableNames.isEmpty()) {
      throw new IllegalStateException("Tables not found."); // Throws an exception if no tables are found
    }

    return tableNames;
  }
}

