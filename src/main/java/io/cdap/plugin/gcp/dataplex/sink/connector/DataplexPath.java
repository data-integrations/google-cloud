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

package io.cdap.plugin.gcp.dataplex.sink.connector;

import java.util.Arrays;

import javax.annotation.Nullable;

/**
 * Dataplex Path that parses the path in the request of connection service
 * A valid path can start with/without a slash, e.g. "/" , "", "/location/lake/zone/asset" are all valid.
 * A valid path can end with/without a slash, e.g. "/", "", "/location/lake/zone/asset/", "location/lake/zone/asset/"
 * are all valid. A valid path should contain at most four parts separated by slash, the first part is location id,
 * the second part is lake id, the third part is zone id and the fourth part is asset id. Consecutive slashes are not
 * valid , it will be parsed as there is an empty string part between the slashes. e.g. "//a" will be parsed as
 * dataset name as empty and table name as "a". Similarly "/a//" will be parsed as dataset
 * name as "a" and table name as empty.
 */
public class DataplexPath {
  private static final int NAME_MAX_LENGTH = 1024;
  private String location;
  private String lake;
  private String zone;
  private String asset;
  private String objectName;

  public DataplexPath(String path) {
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

    location = parts[0];
    validateName("Location", location);

    if (parts.length >= 2) {
      lake = parts[1];
      validateName("Lake", lake);
    }

    if (parts.length >= 3) {
      zone = parts[2];
      validateName("Zone", zone);
    }
    if (parts.length >= 4) {
      asset = parts[3];
      validateName("Asset", asset);
    }
    if (parts.length >= 5) {
      objectName =  String.join("/", Arrays.asList(parts).subList(4, parts.length));
    }
  }


  /**
   * The location, lake, asset and zone name must not be empty
   * and it must be 1024 characters or fewer.
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
  }

  @Nullable
  public String getLake() {
    return lake;
  }

  @Nullable
  public String getZone() {
    return zone;
  }

  @Nullable
  public String getAsset() {
    return asset;
  }

  public String getLocation() {
    return location;
  }

  public String getObjectName() {
    return objectName;
  }
}
