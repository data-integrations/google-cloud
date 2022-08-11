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

package io.cdap.plugin.gcp.bigtable.common;

/**
 * This class represents a reference to HBase column
 */
public class HBaseColumn {
  private static final String FAMILY_QUALIFIER_DELIMITER = ":";
  private final String family;
  private final String qualifier;

  private HBaseColumn(String family, String qualifier) {
    this.family = family;
    this.qualifier = qualifier;
  }

  public static HBaseColumn fromFamilyAndQualifier(String family, String qualifier) {
    return new HBaseColumn(family, qualifier);
  }

  /**
   * Parses the provided full name and returns hbase column with family and qualifier.
   *
   * @param fullName full name
   * @return hbase column with family and qualifier
   * @throws IllegalArgumentException if provided full name does not comply with 'family:qualifier' format
   */
  public static HBaseColumn fromFullName(String fullName) {
    if (!fullName.contains(FAMILY_QUALIFIER_DELIMITER)) {
      throw new IllegalArgumentException("Wrong name format. Expected format is 'family:qualifier'");
    }
    int delimiterIndex = fullName.indexOf(FAMILY_QUALIFIER_DELIMITER);
    String family = fullName.substring(0, delimiterIndex);
    String qualifier = fullName.substring(delimiterIndex + FAMILY_QUALIFIER_DELIMITER.length());
    return new HBaseColumn(family, qualifier);
  }

  public String getFamily() {
    return family;
  }

  public String getQualifier() {
    return qualifier;
  }

  public String getQualifiedName() {
    return String.format("%s%s%s", family, FAMILY_QUALIFIER_DELIMITER, qualifier);
  }
}
