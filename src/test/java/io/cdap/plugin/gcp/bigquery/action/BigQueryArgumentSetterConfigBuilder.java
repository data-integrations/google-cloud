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

package io.cdap.plugin.gcp.bigquery.action;

/**
 * Builder for testing BigQueryArgumentSetterConfig
 */
public final class BigQueryArgumentSetterConfigBuilder {

  private String dataset;
  private String table;
  private String argumentSelectionConditions;
  private String argumentsColumns;

  private BigQueryArgumentSetterConfigBuilder() {
  }

  public static BigQueryArgumentSetterConfigBuilder bigQueryArgumentSetterConfig() {
    return new BigQueryArgumentSetterConfigBuilder();
  }

  public BigQueryArgumentSetterConfigBuilder setDataset(String dataset) {
    this.dataset = dataset;
    return this;
  }

  public BigQueryArgumentSetterConfigBuilder setTable(String table) {
    this.table = table;
    return this;
  }

  public BigQueryArgumentSetterConfigBuilder setArgumentSelectionConditions(
    String argumentSelectionConditions) {
    this.argumentSelectionConditions = argumentSelectionConditions;
    return this;
  }

  public BigQueryArgumentSetterConfigBuilder setArgumentsColumns(String argumentsColumns) {
    this.argumentsColumns = argumentsColumns;
    return this;
  }

  public BigQueryArgumentSetterConfig build() {
    return new BigQueryArgumentSetterConfig(dataset, table, argumentSelectionConditions, argumentsColumns);
  }
}
