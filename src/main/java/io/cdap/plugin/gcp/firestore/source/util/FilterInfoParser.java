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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The class that provides helper method for parsing the filterInfo string.
 */
public class FilterInfoParser {
  /**
   * Returns the filters to apply. Returns an empty list if filters contains a macro. Otherwise,
   * the list returned can never be empty.
   * @param filterString the filter string in value:operator(field)[,value:operator(field)] format
   * @return the list of filters to apply. Returns an empty list if filters contains a macro. Otherwise,
   * the list returned can never be empty
   * @throws IllegalArgumentException
   */
  public static List<FilterInfo> parseFilterString(String filterString) throws IllegalArgumentException {
    List<FilterInfo> filterInfos = new ArrayList<>();

    if (Strings.isNullOrEmpty(filterString)) {
      return filterInfos;
    }
    
    for (String filter : Splitter.on(',').trimResults().split(filterString)) {
      int colonIdx = filter.indexOf(':');
      if (colonIdx < 0) {
        throw new IllegalArgumentException(String.format(
          "Could not find ':' separating filter value from its operation in '%s'.", filter));
      }
      String value = filter.substring(0, colonIdx).trim();

      String opertorAndField = filter.substring(colonIdx + 1).trim();
      int leftParanIdx = opertorAndField.indexOf('(');
      if (leftParanIdx < 0) {
        throw new IllegalArgumentException(String.format(
          "Could not find '(' in operation '%s'. Operations must be specified as operator(field).", opertorAndField));
      }

      String operatorStr = opertorAndField.substring(0, leftParanIdx).trim();
      FilterOperator operator;
      Optional<FilterOperator> optional = FilterOperator.fromValue(operatorStr);
      if (!optional.isPresent()) {
        throw new IllegalArgumentException(String.format(
          "Invalid operator '%s'. Must be one of %s.", operatorStr,
          FilterOperator.getSupportedOperators()));
      }
      operator = optional.get();

      if (!opertorAndField.endsWith(")")) {
        throw new IllegalArgumentException(String.format(
          "Could not find closing ')' in operation '%s'. Operations must be specified as operator(field).",
          opertorAndField));
      }
      String field = opertorAndField.substring(leftParanIdx + 1, opertorAndField.length() - 1).trim();
      if (field.isEmpty()) {
        throw new IllegalArgumentException(String.format(
          "Invalid operation format '%s'. A field must be given as an argument.", opertorAndField));
      }

      Object fieldValue = value;
      if (operator != FilterOperator.EQUAL_TO) {
        try {
          fieldValue = getNumericValue(value);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(String.format(
            "Invalid value '%s' for operator '%s'. A numeric value expected for the operator used.", value,
            operatorStr));
        }
      }

      filterInfos.add(new FilterInfo(field, operator, fieldValue));
    }
    return filterInfos;
  }

  private static Number getNumericValue(String value) throws NumberFormatException {
    Number number = null;

    if (value.indexOf(".") >= 0) {
      number = Double.valueOf(value);
    } else {
      number = Long.valueOf(value);
    }

    return number;
  }
}
