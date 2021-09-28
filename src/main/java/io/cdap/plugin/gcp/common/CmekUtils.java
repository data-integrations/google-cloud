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

package io.cdap.plugin.gcp.common;

import com.google.api.pathtemplate.ValidationException;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.etl.api.Arguments;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.gcs.GCSPath;

import javax.annotation.Nullable;


/**
 *  Cmek Key Utility class to validate cmek in gcp plugins.
 */
public class CmekUtils {
  /**
   * This method checks if the GCS bucket exists.
   * If not, it checks that the CMEK key is in the same location that the bucket will be created in.
   * If there is any exception when checking for bucket existence, it will be ignored.
   *
   * @param storage GCS storage
   * @param gcsPath the path of bucket to validate
   * @param cmekKey the cmek key name
   * @param location the location to create the bucket in if it doesn't exists
   * @param collector failure collector
   */
  public static void validateCmekKeyAndBucketLocation(Storage storage, GCSPath gcsPath, CryptoKeyName cmekKey,
                                                      @Nullable String location, FailureCollector collector) {
    Bucket bucket = null;
    try {
      bucket = storage.get(gcsPath.getBucket());
    } catch (StorageException e) {
      /* Ignoring the exception because we don't want the validation to fail if there is an exception getting
      the bucket information either because the service account used during validation can be different than
      the service account that will be used at runtime (the dataproc service account)
      (assuming the user has auto-detect for the service account) */
      return;
    }
    if (bucket == null) {
      String cmekKeyLocation = cmekKey.getLocation();
      if ((Strings.isNullOrEmpty(location) && !"US".equalsIgnoreCase(cmekKeyLocation))
        || (!Strings.isNullOrEmpty(location) && !cmekKeyLocation.equalsIgnoreCase(location))) {
        String bucketLocation = location;
        if (Strings.isNullOrEmpty(bucketLocation)) {
          bucketLocation = "US";
        }
        collector.addFailure(String.format("CMEK key '%s' is in location '%s' while the GCS bucket '%s' will"
                                             + " be created in location '%s'.", cmekKey,
                                           cmekKeyLocation, gcsPath.getBucket(), bucketLocation)
          , "Modify the CMEK key or bucket location to be the same")
          .withConfigProperty(GCPConfig.NAME_CMEK_KEY);
      }
    }
  }

  /**
   * This method validates the cmek key formatted string.
   *
   * @param cmekKey cmek key raw string
   * @param arguments runtime arguments
   * @param collector  failure collector
   * @return parsed CryptoKeyName object if the formatted string is valid otherwise null.
   */
  @Nullable
  public static CryptoKeyName getCmekKey(@Nullable String cmekKey, @Nullable Arguments arguments,
                                         FailureCollector collector) {
    if (Strings.isNullOrEmpty(cmekKey)) {
      cmekKey = arguments == null ? null : arguments.get("gcp.cmek.key.name");
    }
    CryptoKeyName cmekKeyName = null;
    if (Strings.isNullOrEmpty(cmekKey)) {
      return cmekKeyName;
    }
    try {
      cmekKeyName = CryptoKeyName.parse(cmekKey);
    } catch (ValidationException e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(GCPConfig.NAME_CMEK_KEY).withStacktrace(e.getStackTrace());
    }
    return cmekKeyName;
  }
}
