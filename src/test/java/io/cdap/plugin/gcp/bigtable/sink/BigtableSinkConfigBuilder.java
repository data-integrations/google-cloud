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

package io.cdap.plugin.gcp.bigtable.sink;

public final class BigtableSinkConfigBuilder {
  private String referenceName;
  private String project;
  private String serviceFilePath;
  private String table;
  private String instance;
  private String keyAlias;
  private String columnMappings;
  private String bigtableOptions;

  private BigtableSinkConfigBuilder() {
  }

  public static BigtableSinkConfigBuilder aBigtableSinkConfig() {
    return new BigtableSinkConfigBuilder();
  }

  public BigtableSinkConfigBuilder setProject(String project) {
    this.project = project;
    return this;
  }

  public BigtableSinkConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public BigtableSinkConfigBuilder setServiceFilePath(String serviceFilePath) {
    this.serviceFilePath = serviceFilePath;
    return this;
  }

  public BigtableSinkConfigBuilder setTable(String table) {
    this.table = table;
    return this;
  }

  public BigtableSinkConfigBuilder setInstance(String instance) {
    this.instance = instance;
    return this;
  }

  public BigtableSinkConfigBuilder setKeyAlias(String keyAlias) {
    this.keyAlias = keyAlias;
    return this;
  }

  public BigtableSinkConfigBuilder setColumnMappings(String columnMappings) {
    this.columnMappings = columnMappings;
    return this;
  }

  public BigtableSinkConfigBuilder setBigtableOptions(String bigtableOptions) {
    this.bigtableOptions = bigtableOptions;
    return this;
  }

  public BigtableSinkConfig build() {
    return new BigtableSinkConfig(
      referenceName, 
      table, 
      instance, 
      project, 
      serviceFilePath, 
      keyAlias, 
      columnMappings, 
      bigtableOptions
    );
  }
}
