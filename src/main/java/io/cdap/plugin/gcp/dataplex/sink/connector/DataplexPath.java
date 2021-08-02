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
public class DataplexPath {
    private static final int NAME_MAX_LENGTH = 1024;
    private static final String VALID_NAME_REGEX = "[\\w]+";
    private String lake;
    private String zone;
    private String asset;

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

        // path should contain at most two part : dataset and table
        if (parts.length > 3) {
            throw new IllegalArgumentException("Path should at most contain three parts.");
        }

        lake = parts[0];
        validateName("Lake", lake);

        if (parts.length == 2) {
            zone = parts[1];
            validateName("Zone", zone);
        }
        if (parts.length == 3) {
            asset = parts[2];
            validateName("Asset", asset);
        }
    }


    /**
     * The dataset and table name must contain only letters, numbers, and underscores.
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
        if (!name.matches(VALID_NAME_REGEX)) {
            throw new IllegalArgumentException(
              String.format("%s is invalid, it should contain only letters, numbers, and underscores.", property));
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
}
