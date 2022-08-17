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
package io.cdap.plugin.gcp.datastore.sink.util;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Indicates index strategy for provided sink schema fields.
 */
public enum IndexStrategy {

  /**
   * All properties will be included to index.
   */
  ALL("All"),

  /**
   * All properties will be excluded from index.
   */
  NONE("None"),

  /**
   * Indexed properties will be provided.
   */
  CUSTOM("Custom");

  private final String value;

  IndexStrategy(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  /**
   * Converts index strategy string value into {@link IndexStrategy} enum.
   *
   * @param stringValue index strategy string value
   * @return index strategy in optional container
   */
  public static Optional<IndexStrategy> fromValue(String stringValue) {
    return Stream.of(values())
      .filter(indexStrategy -> indexStrategy.value.equalsIgnoreCase(stringValue))
      .findAny();
  }

  public static String getSupportedStrategies() {
    return Arrays.stream(IndexStrategy.values()).map(IndexStrategy::getValue).collect(Collectors.joining(", "));
  }
}
