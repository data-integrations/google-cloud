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

package io.cdap.plugin.gcp.spanner.connector;

import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Spanner path
 */
public class SpannerPath {
  private String instance;
  private String database;
  private String table;
  private static final int NAME_MAX_LENGTH = 1024;
  private static final Pattern VALID_NAME_REGEX = Pattern.compile("[\\w-]+");

  public SpannerPath(String path) {
    parsePath(path);
  }

  private void parsePath(String path) {
    if (path == null) {
      throw new IllegalArgumentException("Path should not be null.");
    }

    // remove heading "/" if exists
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

    // path should contain at most three part : instance, database and table
    if (parts.length > 3) {
      throw new IllegalArgumentException("Path should at most contain three parts.");
    }

    instance = parts[0];
    validateName("Instance" , instance);

    if (parts.length == 1) {
      // parts only contains instance name
      return;
    }

    database = parts[1];
    validateName("Database", database);

    if (parts.length == 3) {
      table = parts[2];
      validateName("Table", table);
    }
  }

  /**
   * The instance, database and table name must contain only letters, numbers, hyphens and underscores.
   * And it must be 1024 characters or fewer.
   */
  private void validateName(String property, String name) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException(
        String.format("%s should not be empty.", property));
    }
    if (name.length() > NAME_MAX_LENGTH) {
      throw new IllegalArgumentException(
        String.format("%s is invalid, it should contain at most %d characters.", property, NAME_MAX_LENGTH));
    }
    if (!VALID_NAME_REGEX.matcher(name).matches()) {
      throw new IllegalArgumentException(
        String.format("%s is invalid, it should contain only letters, numbers, and underscores.", property));
    }
  }

  @Nullable
  public String getInstance() {
    return instance;
  }

  @Nullable
  public String getDatabase() {
    return database;
  }

  @Nullable
  public String getTable() {
    return table;
  }

  public boolean isRoot() {
    return instance == null && database == null && table == null;
  }
}
