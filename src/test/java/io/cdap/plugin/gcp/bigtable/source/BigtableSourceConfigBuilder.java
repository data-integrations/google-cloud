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

package io.cdap.plugin.gcp.bigtable.source;

public final class BigtableSourceConfigBuilder {
  private String referenceName;
  private String project;
  private String serviceFilePath;
  private String table;
  private String instance;
  private String keyAlias;
  private String columnMappings;
  private String scanRowStart;
  private String scanRowStop;
  private Long scanTimeRangeStart;
  private Long scanTimeRangeStop;
  private String onError;
  private String schema;
  private String bigtableOptions;

  private BigtableSourceConfigBuilder() {
  }

  public static BigtableSourceConfigBuilder aBigtableSourceConfig() {
    return new BigtableSourceConfigBuilder();
  }

  public BigtableSourceConfigBuilder setProject(String project) {
    this.project = project;
    return this;
  }

  public BigtableSourceConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public BigtableSourceConfigBuilder setServiceFilePath(String serviceFilePath) {
    this.serviceFilePath = serviceFilePath;
    return this;
  }

  public BigtableSourceConfigBuilder setTable(String table) {
    this.table = table;
    return this;
  }

  public BigtableSourceConfigBuilder setInstance(String instance) {
    this.instance = instance;
    return this;
  }

  public BigtableSourceConfigBuilder setKeyAlias(String keyAlias) {
    this.keyAlias = keyAlias;
    return this;
  }

  public BigtableSourceConfigBuilder setColumnMappings(String columnMappings) {
    this.columnMappings = columnMappings;
    return this;
  }

  public BigtableSourceConfigBuilder setScanRowStart(String scanRowStart) {
    this.scanRowStart = scanRowStart;
    return this;
  }

  public BigtableSourceConfigBuilder setScanRowStop(String scanRowStop) {
    this.scanRowStop = scanRowStop;
    return this;
  }

  public BigtableSourceConfigBuilder setScanTimeRangeStart(Long scanTimeRangeStart) {
    this.scanTimeRangeStart = scanTimeRangeStart;
    return this;
  }

  public BigtableSourceConfigBuilder setScanTimeRangeStop(Long scanTimeRangeStop) {
    this.scanTimeRangeStop = scanTimeRangeStop;
    return this;
  }

  public BigtableSourceConfigBuilder setBigtableOptions(String bigtableOptions) {
    this.bigtableOptions = bigtableOptions;
    return this;
  }

  public BigtableSourceConfigBuilder setOnError(String onError) {
    this.onError = onError;
    return this;
  }

  public BigtableSourceConfigBuilder setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public BigtableSourceConfig build() {
    return new BigtableSourceConfig(
      referenceName,
      table,
      instance,
      project,
      serviceFilePath,
      keyAlias,
      columnMappings,
      scanRowStart,
      scanRowStop,
      scanTimeRangeStart,
      scanTimeRangeStop,
      bigtableOptions,
      onError,
      schema
    );
  }
}
