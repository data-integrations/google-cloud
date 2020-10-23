/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.gcp.firestore.source.util;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Indicates operator which can be used in filters.
 */
public enum FilterOperator {

  /**
   * Operator will be used to perform "equal to" operation on string values.
   */
  EQUAL_TO("EqualTo"),

  /**
   * Operator will be used to perform "equal to" operation on numeric values.
   */
  NUMERIC_EQUAL_TO("NumericEqualTo"),

  /**
   * Operator will be used to perform "less than" operation on numeric values.
   */
  LESS_THAN("LessThan"),

  /**
   * Operator will be used to perform "less than or equal to" operation on numeric values.
   */
  LESS_THAN_OR_EQUAL_TO("LessThanOrEqualTo"),

  /**
   * Operator will be used to perform "greater than" operation on numeric values.
   */
  GREATER_THAN("GreaterThan"),

  /**
   * Operator will be used to perform "greater than or equal to" operation on numeric values.
   */
  GREATER_THAN_OR_EQUAL_TO("GreaterThanOrEqualTo");

  private final String value;

  FilterOperator(String value) {
    this.value = value;
  }

  /**
   * Converts operator string value into {@link FilterOperator} enum.
   *
   * @param stringValue operator string value
   * @return filter operator in optional container
   */
  public static Optional<FilterOperator> fromValue(String stringValue) {
    return Stream.of(values())
      .filter(keyType -> keyType.value.equalsIgnoreCase(stringValue))
      .findAny();
  }

  public static String getSupportedOperators() {
    return Arrays.stream(FilterOperator.values()).map(FilterOperator::getValue).collect(Collectors.joining(", "));
  }

  public String getValue() {
    return value;
  }
}
