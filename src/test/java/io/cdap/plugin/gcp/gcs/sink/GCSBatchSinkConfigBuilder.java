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

package io.cdap.plugin.gcp.gcs.sink;

public class GCSBatchSinkConfigBuilder {
  private String referenceName;
  private String serviceAccountType;
  private String serviceFilePath;
  private String serviceAccountJson;
  private String project;
  private String gcsPath;
  private String cmekKey;
  private String location;

  public GCSBatchSinkConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public GCSBatchSinkConfigBuilder setProject(String project) {
    this.project = project;
    return this;
  }

  public GCSBatchSinkConfigBuilder setServiceAccountType(String serviceAccountType) {
    this.serviceAccountType = serviceAccountType;
    return this;
  }

  public GCSBatchSinkConfigBuilder setServiceFilePath(String serviceFilePath) {
    this.serviceFilePath = serviceFilePath;
    return this;
  }

  public GCSBatchSinkConfigBuilder setServiceAccountJson(String serviceAccountJson) {
    this.serviceAccountJson = serviceAccountJson;
    return this;
  }

  public GCSBatchSinkConfigBuilder setGcsPath(String gcsPath) {
    this.gcsPath = gcsPath;
    return this;
  }

  public GCSBatchSinkConfigBuilder setCmekKey(String cmekKey) {
    this.cmekKey = cmekKey;
    return this;
  }

  public GCSBatchSinkConfigBuilder setLocation(String location) {
    this.location = location;
    return this;
  }

  public static GCSBatchSinkConfigBuilder aGCSBatchSinkConfig() {
    return new GCSBatchSinkConfigBuilder();
  }

  public GCSBatchSink.GCSBatchSinkConfig build() {
    return new GCSBatchSink.GCSBatchSinkConfig(
      referenceName,
      project,
      serviceAccountType,
      serviceFilePath,
      serviceAccountJson,
      gcsPath,
      location,
      cmekKey
    );
  }

}
