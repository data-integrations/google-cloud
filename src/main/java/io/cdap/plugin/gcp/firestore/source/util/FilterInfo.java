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

/**
 * Class to hold information for a filter.
 */
public class FilterInfo {
  private String field;
  private FilterOperator operator;
  private Object value;

  /**
   * Constructor for FilterInfo object.
   * @param field the field
   * @param operator the operator
   * @param value the value
   */
  public FilterInfo(String field, FilterOperator operator, Object value) {
    this.field = field;
    this.operator = operator;
    this.value = value;
  }

  public String getField() {
    return field;
  }

  public FilterOperator getOperator() {
    return operator;
  }

  public Object getValue() {
    return value;
  }
}
