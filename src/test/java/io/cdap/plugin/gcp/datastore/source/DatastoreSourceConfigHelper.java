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
package io.cdap.plugin.gcp.datastore.source;

import io.cdap.plugin.gcp.datastore.source.util.SourceKeyType;

/**
 * Utility class that provides handy methods to construct Datastore Source Config for testing
 */
public class DatastoreSourceConfigHelper {

  public static final String TEST_REF_NAME = "TestRefName";
  public static final String TEST_PROJECT = "test-project";
  public static final String TEST_NAMESPACE = "TestNamespace";
  public static final String TEST_KIND = "TestKind";

  public static ConfigBuilder newConfigBuilder() {
    return new ConfigBuilder();
  }

  public static class ConfigBuilder {
    private String referenceName = TEST_REF_NAME;
    private String project = TEST_PROJECT;
    private String serviceFilePath = "/path/to/file";
    private String namespace = TEST_NAMESPACE;
    private String kind = TEST_KIND;
    private String ancestor;
    private String filters;
    private int numSplits = 1;
    private String keyType = SourceKeyType.NONE.getValue();
    private String keyAlias;
    private String schema;

    public ConfigBuilder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public ConfigBuilder setProject(String project) {
      this.project = project;
      return this;
    }

    public ConfigBuilder setServiceFilePath(String serviceFilePath) {
      this.serviceFilePath = serviceFilePath;
      return this;
    }

    public ConfigBuilder setNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public ConfigBuilder setKind(String kind) {
      this.kind = kind;
      return this;
    }

    public ConfigBuilder setAncestor(String ancestor) {
      this.ancestor = ancestor;
      return this;
    }

    public ConfigBuilder setFilters(String filters) {
      this.filters = filters;
      return this;
    }

    public ConfigBuilder setNumSplits(int numSplits) {
      this.numSplits = numSplits;
      return this;
    }

    public ConfigBuilder setKeyType(String keyType) {
      this.keyType = keyType;
      return this;
    }

    public ConfigBuilder setKeyAlias(String keyAlias) {
      this.keyAlias = keyAlias;
      return this;
    }

    public ConfigBuilder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public DatastoreSourceConfig build() {
      return new DatastoreSourceConfig(referenceName, project, serviceFilePath, namespace, kind, ancestor,
                                       filters, numSplits, keyType, keyAlias, schema);
    }

  }

}
