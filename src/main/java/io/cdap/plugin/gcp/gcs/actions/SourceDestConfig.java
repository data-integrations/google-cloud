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

package io.cdap.plugin.gcp.gcs.actions;

import com.google.api.pathtemplate.ValidationException;
import com.google.auth.Credentials;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Contains common properties for copy/move.
 */
public class SourceDestConfig extends GCPConfig {
  public static final String NAME_SOURCE_PATH = "sourcePath";
  public static final String NAME_DEST_PATH = "destPath";
  public static final String NAME_LOCATION = "location";
  public static final String READ_TIMEOUT = "readTimeout";

  @Name(NAME_SOURCE_PATH)
  @Macro
  @Description("Path to a source object or directory.")
  private String sourcePath;

  @Name(NAME_DEST_PATH)
  @Macro
  @Description("Path to the destination. The bucket will be created if it does not exist.")
  private String destPath;

  @Macro
  @Nullable
  @Description("Whether to overwrite existing objects.")
  private Boolean overwrite;

  @Name(NAME_LOCATION)
  @Macro
  @Nullable
  @Description("The location where the GCS bucket will get created. " +
    "This value is ignored if the bucket already exists.")
  protected String location;

  @Name(NAME_CMEK_KEY)
  @Macro
  @Nullable
  @Description("The GCP customer managed encryption key (CMEK) name used to encrypt data written to " +
    "any bucket created by the plugin. If the bucket already exists, this is ignored. More information can be found" +
    " at https://cloud.google.com/data-fusion/docs/how-to/customer-managed-encryption-keys")
  protected String cmekKey;

  @Name(READ_TIMEOUT)
  @Macro
  @Nullable
  @Description("Timeout in seconds to read data from an established HTTP connection (Default value is 20). " +
    ("For performing copy/move operation on large files in GCS buckets, set a higher value. " +
      "Set it to 0 for infinite(no limit)"))
  protected Integer readTimeout;

  public SourceDestConfig(@Nullable String project, @Nullable String serviceAccountType,
                          @Nullable String serviceFilePath, @Nullable String serviceAccountJson,
                          @Nullable String destPath, @Nullable String location, @Nullable Integer readTimeout,
                          @Nullable String cmekKey) {
    this.serviceAccountType = serviceAccountType;
    this.serviceAccountJson = serviceAccountJson;
    this.serviceFilePath = serviceFilePath;
    this.project = project;
    this.destPath = destPath;
    this.location = location;
    this.readTimeout = readTimeout;
    this.cmekKey = cmekKey;
  }

  public SourceDestConfig() {
    overwrite = false;
  }

  GCSPath getSourcePath() {
    return GCSPath.from(sourcePath);
  }

  GCSPath getDestPath() {
    return GCSPath.from(destPath);
  }

  @Nullable
  Boolean shouldOverwrite() {
    return overwrite;
  }

  public void validate(FailureCollector collector) {
    validate(collector, Collections.emptyMap());
  }
  
  public void validate(FailureCollector collector, Map<String, String> arguments) {
    if (!containsMacro("sourcePath")) {
      try {
        getSourcePath();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_SOURCE_PATH);
      }
    }
    if (!containsMacro("destPath")) {
      try {
        getDestPath();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_DEST_PATH);
      }
    }
    if (!containsMacro(NAME_CMEK_KEY)) {
      validateCmekKey(collector, arguments);
    }
    if (!containsMacro(READ_TIMEOUT)) {
      validateReadTimeout(collector);
    }
    collector.getOrThrowException();
  }

  void validateReadTimeout(FailureCollector collector) {
    if (readTimeout == null) {
      return;
    }
    if (readTimeout < 0) {
      collector.addFailure("Read Timeout cannot be less than 0. ",
                           "Please enter 0 or a positive value.").withConfigProperty(READ_TIMEOUT);
    }
  }

  //This method validated the pattern of CMEK Key resource ID.
  void validateCmekKey(FailureCollector failureCollector, Map<String, String> arguments) {
    CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(cmekKey, arguments, failureCollector);

    //these fields are needed to check if bucket exists or not and for location validation
    if (cmekKeyName == null || containsMacro(NAME_DEST_PATH) || containsMacro(NAME_LOCATION) ||
      projectOrServiceAccountContainsMacro()) {
      return;
    }
    
    Storage storage = GCPUtils.getStorage(getProject(), getCredentials(failureCollector));
    if (storage == null) {
      return;
    }
    CmekUtils.validateCmekKeyAndBucketLocation(storage, GCSPath.from(destPath),
                                               cmekKeyName, location, failureCollector);
  }

  public static SourceDestConfig.Builder builder() {
    return new SourceDestConfig.Builder();
  }

  /**
   * SourceDest configuration builder.
   */
  public static class Builder {
    private String serviceAccountType;
    private String serviceFilePath;
    private String serviceAccountJson;
    private String project;
    private String destPath;
    private String cmekKey;
    private String location;
    private Integer readTimeout;

    public SourceDestConfig.Builder setProject(@Nullable String project) {
      this.project = project;
      return this;
    }

    public SourceDestConfig.Builder setServiceAccountType(@Nullable String serviceAccountType) {
      this.serviceAccountType = serviceAccountType;
      return this;
    }

    public SourceDestConfig.Builder setServiceFilePath(@Nullable String serviceFilePath) {
      this.serviceFilePath = serviceFilePath;
      return this;
    }

    public SourceDestConfig.Builder setServiceAccountJson(@Nullable String serviceAccountJson) {
      this.serviceAccountJson = serviceAccountJson;
      return this;
    }

    public SourceDestConfig.Builder setGcsPath(@Nullable String destPath) {
      this.destPath = destPath;
      return this;
    }

    public SourceDestConfig.Builder setCmekKey(@Nullable String cmekKey) {
      this.cmekKey = cmekKey;
      return this;
    }

    public SourceDestConfig.Builder setLocation(@Nullable String location) {
      this.location = location;
      return this;
    }

    public SourceDestConfig build() {
      return new SourceDestConfig(
        project,
        serviceAccountType,
        serviceFilePath,
        serviceAccountJson,
        destPath,
        location,
        readTimeout,
        cmekKey
      );
    }
  }
}
