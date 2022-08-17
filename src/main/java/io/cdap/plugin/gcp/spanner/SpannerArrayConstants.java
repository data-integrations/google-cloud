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

package io.cdap.plugin.gcp.spanner;

/**
 * Spanner array constants
 */
public final class SpannerArrayConstants {
  public static final String ARRAY_INT64 = "ARRAY<INT64>";
  public static final String ARRAY_FLOAT64 = "ARRAY<FLOAT64>";
  public static final String ARRAY_BOOL = "ARRAY<BOOL>";
  public static final String ARRAY_DATE = "ARRAY<DATE>";
  public static final String ARRAY_TIMESTAMP = "ARRAY<TIMESTAMP>";
  public static final String ARRAY_STRING_PREFIX = "ARRAY<STRING";
  public static final String ARRAY_BYTES_PREFIX = "ARRAY<BYTES";
}
