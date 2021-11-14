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

package io.cdap.plugin.gcp.bigquery.util;

/**
 * Stores the Precision and Scale for the different types
 * provided by BigQuery
 */
public final class BigQueryTypeSize {

  /**
   * Precision and scale of BigQuery Numeric class
   *  * https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_types
   */
  public static final class Numeric {
    public static final int PRECISION = 38;
    public static final int SCALE = 9;
  }

  /**
   * Precision and Scale of BigQuery BigNumeric class
   * https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_types
   */
  public static final class BigNumeric {
    public static final int PRECISION = 77;
    public static final int SCALE = 38;
  }

  /**
   * Maxium depth of Bigquery nested Struct
   * https://cloud.google.com/bigquery/docs/nested-repeated
   */
  public static final class Struct {
    public static final int MAX_DEPTH = 15;
  }

  private BigQueryTypeSize() {
    // Avoid the class to ever being called
    throw new AssertionError();
  }

}
