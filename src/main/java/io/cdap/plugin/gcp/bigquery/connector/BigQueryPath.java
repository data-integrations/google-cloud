/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.connector;

import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * BigQuery Path that parses the path in the request of connection service
 * A valid path can start with/without a slash, e.g. "/" , "", "/dataset/table", "dataset/table" are all valid.
 * A valid path can end with/without a slash, e.g. "/", "", "/dataset/table/", "dataset/table/" are all valid.
 * A valid path should contain at most two parts separated by slash, the first part is dataset name and the second
 * part is table name. Both of them are optional. e.g. "/" , "/dataset" , "/dataset/table".
 * Consecutive slashes are not valid , it will be parsed as there is an empty string part between the slashes. e.g.
 * "//a" will be parsed as dataset name as empty and table name as "a". Similarly "/a//" will be parsed as dataset
 * name as "a" and table name as empty.
 *
 */
public class BigQueryPath {
  private String dataset;
  private String table;
  private static final int NAME_MAX_LENGTH = 1024;
  // Valid BigQuery dataset names can contain only letters, numbers, and underscores.
  // See here: https://cloud.google.com/bigquery/docs/datasets#dataset-naming
  private static final Pattern VALID_DATASET_NAME_REGEX = Pattern.compile("[\\w]+");
  // Valid BigQuery table names are defined here: https://cloud.google.com/bigquery/docs/tables#table_naming
  private static final Pattern VALID_TABLE_NAME_REGEX = Pattern.compile("[\\p{L}\\p{M}\\p{N}\\p{Pc}\\p{Pd}\\p{Zs}]+");

  public BigQueryPath(String path) {
    parsePath(path);
  }

  private void parsePath(String path) {
    if (path == null) {
      throw new IllegalArgumentException("Path should not be null.");
    }

    //remove heading "/" if exists
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    // both "" and "/" are taken as root path
    if (path.isEmpty()) {
      return;
    }

    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    String[] parts = path.split("/", -1);

    // path should contain at most two part : dataset and table
    if (parts.length > 2) {
      throw new IllegalArgumentException("Path should at most contain two parts.");
    }

    dataset = parts[0];
    validateDatasetName(dataset);

    if (parts.length == 2) {
      table = parts[1];
      validateTableName(table);
    }
  }


  /**
   * The dataset name can contain only letters, numbers, and underscores, and must be 1024 characters or fewer.
   * See here: https://cloud.google.com/bigquery/docs/datasets#dataset-naming
   */
  private void validateDatasetName(String name) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException("Dataset should not be empty.");
    }
    if (name.length() > NAME_MAX_LENGTH) {
      throw new IllegalArgumentException(
        String.format("Dataset is invalid, it should contain at most %d characters.", NAME_MAX_LENGTH));
    }
    if (!VALID_DATASET_NAME_REGEX.matcher(name).matches()) {
      throw new IllegalArgumentException("Dataset is invalid, it should contain only letters, numbers, " +
        "and underscores.");
    }
  }

  /**
   * Table name can contain only Unicode characters in category L (letter), M (mark), N (number),
   * Pc (connector, including underscore), Pd (dash), Zs (space).
   * It also must be 1024 characters or fewer.
   * See here: https://cloud.google.com/bigquery/docs/tables#table_naming
   */
  private void validateTableName(String name) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException("Table should not be empty.");
    }
    if (name.length() > NAME_MAX_LENGTH) {
      throw new IllegalArgumentException(
        String.format("Table is invalid, it should contain at most %d characters.", NAME_MAX_LENGTH));
    }
    if (!VALID_TABLE_NAME_REGEX.matcher(name).matches()) {
      throw new IllegalArgumentException("Table is invalid, it should only contain Unicode characters in category L " +
        "(letter), M (mark), N (number), Pc (connector, including underscore), Pd (dash), Zs (space).");
    }
  }

  @Nullable
  public String getDataset() {
    return dataset;
  }

  @Nullable
  public String getTable() {
    return table;
  }
}
