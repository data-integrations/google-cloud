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
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;

import javax.annotation.Nullable;

/**
 * Contains common properties for copy/move.
 */
public class SourceDestConfig extends GCPConfig {
  public static final String NAME_SOURCE_PATH = "sourcePath";
  public static final String NAME_DEST_PATH = "destPath";
  public static final String NAME_LOCATION = "location";

  @Name(NAME_SOURCE_PATH)
  @Macro
  @Description("Path to a source object or directory.")
  private String sourcePath;

  @Name(NAME_DEST_PATH)
  @Macro
  @Description("Path to the destination. The bucket must already exist.")
  private String destPath;

  @Macro
  @Nullable
  @Description("Whether to overwrite existing objects.")
  private Boolean overwrite;

  @Name(NAME_LOCATION)
  @Macro
  @Nullable
  @Description("The location where the gcs buckets will get created. " +
    "This value is ignored if the bucket already exists.")
  protected String location;

  @Name(NAME_CMEK_KEY)
  @Macro
  @Nullable
  @Description("The GCP customer managed encryption key (CMEK) name used by Cloud Dataproc")
  private String cmekKey;

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
    if (!containsMacro(NAME_CMEK_KEY) && !Strings.isNullOrEmpty(cmekKey)) {
      validateCmekKey(collector);
    }
    collector.getOrThrowException();
  }

  //This method validated the pattern of CMEK Key resource ID.
  void validateCmekKey(FailureCollector failureCollector) {
    CryptoKeyName cmekKeyName = null;
    try {
      cmekKeyName = CryptoKeyName.parse(cmekKey);
    } catch (ValidationException e) {
      failureCollector.addFailure(e.getMessage(), null)
        .withConfigProperty(NAME_CMEK_KEY).withStacktrace(e.getStackTrace());
      return;
    }

    //these fields are needed to check if bucket exists or not and for location validation
    if (containsMacro(NAME_DEST_PATH) || containsMacro(NAME_LOCATION) || containsMacro(NAME_SERVICE_ACCOUNT_TYPE)
      || containsMacro(NAME_SERVICE_ACCOUNT_JSON) || containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH)) {
      return;
    }
    Boolean isServiceAccountFilePath = isServiceAccountFilePath();
    Credentials credentials = null;
    try {
      credentials = getServiceAccount() == null ?
        null : GCPUtils.loadServiceAccountCredentials(getServiceAccount(), isServiceAccountFilePath);
    } catch (Exception e) {
        /*Ignoring the exception because we don't want to highlight cmek key if an exception occurs while
        loading credentials*/
      return;
    }
    Storage storage = GCPUtils.getStorage(getProject(), credentials);
    Bucket bucket = null;
    try {
      bucket = storage.get(getDestPath().getBucket());
    } catch (StorageException e) {
        /* Ignoring the exception because we don't want the validation to fail if there is an exception getting
          the bucket information either because the service account used during validation can be different than
          the service account that will be used at runtime (the dataproc service account)
          (assuming the user has auto-detect for the service account) */
      return;
    }
    if (bucket == null) {
      String cmekKeyLocation = cmekKeyName.getLocation();
      if ((Strings.isNullOrEmpty(location) && !"US".equalsIgnoreCase(cmekKeyLocation))
        || (!Strings.isNullOrEmpty(location) && !cmekKeyLocation.equalsIgnoreCase(location))) {
        String bucketLocation = location;
        if (Strings.isNullOrEmpty(bucketLocation)) {
          bucketLocation = "US";
        }
        failureCollector.addFailure(String.format("CMEK key '%s' is in location '%s' while the GCS bucket will " +
                                                    "be created in location '%s'.", cmekKey,
                                                  cmekKeyLocation, bucketLocation)
          , "Modify the CMEK key or bucket location to be the same")
          .withConfigProperty(NAME_CMEK_KEY);
      }
    }
  }
}
