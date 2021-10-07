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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
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
  
  private static final String errorMessageTemplate = "CMEK key '%s' is in location '%s' while the %s '%s' " +
    "will be created in location '%s'.";
  private static final String correctiveActionTemplate = "Modify the CMEK key or %s location to be the same";

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
    if (Strings.isNullOrEmpty(location)) {
      location = "US";
    }
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
      if (!cmekKeyLocation.equalsIgnoreCase(location)) {
        String bucketLocation = location;
        if (Strings.isNullOrEmpty(bucketLocation)) {
          bucketLocation = "US";
        }
        collector.addFailure(String.format(errorMessageTemplate, cmekKey.getCryptoKey(), cmekKeyLocation,
                                           "bucket", gcsPath.getBucket(), bucketLocation)
          , String.format(correctiveActionTemplate, "bucket")).withConfigProperty(GCPConfig.NAME_CMEK_KEY);
      }
    }
  }

  /**
   * This method validates the cmek key formatted string.
   *
   * @param cmekKey cmek key raw string
   * @param collector  failure collector
   * @return parsed CryptoKeyName object if the formatted string is valid otherwise null.
   */
  @Nullable
  public static CryptoKeyName getCmekKey(@Nullable String cmekKey, FailureCollector collector) {
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
  
  /**
   * This method checks if the BigQuery dataset exists.
   * If not, it checks that the CMEK key is in the same location that the dataset will be created in.
   * If there is any exception when checking for dataset existence, it will be ignored.
   *
   * @param bigQuery  the bigquery client for the project
   * @param datasetId the id of the dataset
   * @param tableId the id of the table
   * @param cmekKey the cmek key name
   * @param location the location to create the dataset in if it does not exists
   * @param collector failure collector
   * @return the dataset if it exists otherwise null
   */
  public static Dataset validateCmekKeyAndDatasetOrTableLocation(BigQuery bigQuery, DatasetId datasetId,
                                                                 @Nullable TableId tableId, CryptoKeyName cmekKey,
                                                                 @Nullable String location,
                                                                 FailureCollector collector) {
    Dataset dataset = null;
    Table table = null;
    if (Strings.isNullOrEmpty(location)) {
      location = "US";
    }
    try {
      dataset = bigQuery.getDataset(datasetId);
      table = tableId == null ? null : bigQuery.getTable(tableId);
    } catch (BigQueryException e) {
      /* Ignoring the exception because we don't want the validation to fail if there is an exception getting
      the dataset or table information either because the service account used during validation can be different than
      the service account that will be used at runtime (the dataproc service account)
      (assuming the user has auto-detect for the service account) */
      return dataset;
    }
    String cmekKeyLocation = cmekKey.getLocation();
    if (dataset == null) {
      if (!cmekKeyLocation.equalsIgnoreCase(location)) {
        collector.addFailure(String.format(errorMessageTemplate, cmekKey.getCryptoKey(), cmekKeyLocation,
                                           "dataset", datasetId.getDataset(), location)
          , String.format(correctiveActionTemplate, "dataset")).withConfigProperty(GCPConfig.NAME_CMEK_KEY);
      }
    } else if (table == null) {
      location = dataset.getLocation();
      if (!cmekKeyLocation.equalsIgnoreCase(location)) {
        collector.addFailure(String.format(errorMessageTemplate, cmekKey.getCryptoKey(),
                                           cmekKeyLocation, "table", tableId.getTable(), location)
          , String.format(correctiveActionTemplate, "table")).withConfigProperty(GCPConfig.NAME_CMEK_KEY);
      }
    }
    return dataset;
  }
}

