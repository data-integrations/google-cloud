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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.gcs.GCSPath;

import javax.annotation.Nullable;


/**
 *  Cmek Key Utility class to validate cmek in gcp plugins.
 */
public class CmekUtils {

  /**
   * This method validates the cmek formatted string.
   *
   * @param key cmek key formatted string
   * @param collector  failure collector
   * @return parsed CryptoKeyName object.
   */
  public static CryptoKeyName parseCmekKey(String key, FailureCollector collector) {
    CryptoKeyName cmekKeyName = null;
    try {
      cmekKeyName = CryptoKeyName.parse(key);
    } catch (ValidationException e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(GCPConfig.NAME_CMEK_KEY).withStacktrace(e.getStackTrace());
    }
    return cmekKeyName;
  }

  /**
   * This method validates the location of cmek key and bucket if it doesn't exists.
   *
   * @param storage GCS storage
   * @param path the path of bucket to validate
   * @param cmekKey the cmek key name
   * @param location the location to create the bucket in if it doesn't exists
   * @param collector failure collector
   * @return true if bucket does not exist, otherwise false
   */
  public static boolean validateCmekKeyAndBucketLocation(Storage storage, String path, CryptoKeyName cmekKey,
                                                  @Nullable String location, FailureCollector collector) {
    GCSPath gcsPath = GCSPath.from(path);
    Bucket bucket = null;
    try {
      bucket = storage.get(gcsPath.getBucket());
    } catch (StorageException e) {
          /* Ignoring the exception because we don't want the validation to fail if there is an exception getting
          the bucket information either because the service account used during validation can be different than
          the service account that will be used at runtime (the dataproc service account)
          (assuming the user has auto-detect for the service account) */
      return false;
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
      return true;
    }
    return false;
  }
}
