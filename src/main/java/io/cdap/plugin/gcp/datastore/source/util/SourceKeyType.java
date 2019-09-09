/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.plugin.gcp.datastore.source.util;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Indicates key type which will be output along with corresponding entity.
 */
public enum SourceKeyType {

  /**
   * No key will be included in the output.
   */
  NONE("None"),

  /**
   * Key will be represented in the Datastore key literal format including complete path with ancestors.
   * Example: key(<kind>, <identifier>, <kind>, <identifier>, [...])
   */
  KEY_LITERAL("Key literal"),

  /**
   * Key will be represented in the encoded form that can be used as part of a URL.
   */
  URL_SAFE_KEY("URL-safe key");

  private final String value;

  SourceKeyType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  /**
   * Converts key type string value into {@link SourceKeyType} enum.
   *
   * @param stringValue key type string value
   * @return source key type in optional container
   */
  public static Optional<SourceKeyType> fromValue(String stringValue) {
    return Stream.of(values())
      .filter(keyType -> keyType.value.equalsIgnoreCase(stringValue))
      .findAny();
  }

  public static String getSupportedTypes() {
    return Arrays.stream(SourceKeyType.values()).map(SourceKeyType::getValue).collect(Collectors.joining(", "));
  }
}
