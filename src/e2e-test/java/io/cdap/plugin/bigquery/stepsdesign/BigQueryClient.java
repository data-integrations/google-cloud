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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static io.cdap.plugin.bigquery.stepsdesign.BQMultitableValidation.getMysqlConnection;

public class BigQueryClient {

  public static void createSourceDatatypesTable(String sourceTable) throws SQLException, ClassNotFoundException {
    try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement()) {
      String createTableQuery = "CREATE TABLE " + sourceTable +
        "(EmployeeID int, LastName varchar(255), City varchar(255), tablename varchar(255))";
      statement.executeUpdate(createTableQuery);


      // Insert dummy data.
      statement.executeUpdate("INSERT INTO " + sourceTable + " (EmployeeID, lastName, city, tablename)" +
                                "VALUES (1, 'TOM', 'Norway','tabA')");
      statement.executeUpdate("INSERT INTO " + sourceTable + " (EmployeeID, lastName, city, tablename)" +
                                "VALUES (2, 'Shelly', 'Norway','tabA')");
      statement.executeUpdate("INSERT INTO " + sourceTable + " (EmployeeID, lastName, city, tablename)" +
                                "VALUES (3, 'David', 'Norway','tabB')");
      statement.executeUpdate("INSERT INTO " + sourceTable + " (EmployeeID, lastName, city, tablename)" +
                                "VALUES (4, 'Maria', 'Canada','tabB')");

    }
  }
}
