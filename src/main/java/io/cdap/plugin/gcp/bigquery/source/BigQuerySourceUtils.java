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

package io.cdap.plugin.gcp.bigquery.source;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnectorConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Utility class for BigQuery source.
 */
public class BigQuerySourceUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceUtils.class);
  public static final String GCS_BUCKET_FORMAT = "gs://%s";
  public static final String GS_PATH_FORMAT = GCS_BUCKET_FORMAT + "/%s";
  private static final String TEMPORARY_BUCKET_FORMAT = GS_PATH_FORMAT + "/hadoop/input/%s";
  private static final String BQ_TEMP_BUCKET_NAME_PREFIX = "bq-source-bucket-";
  private static final String BQ_TEMP_BUCKET_NAME_TEMPLATE = BQ_TEMP_BUCKET_NAME_PREFIX + "%s";
  private static final String BQ_TEMP_BUCKET_PATH_TEMPLATE = "gs://" + BQ_TEMP_BUCKET_NAME_TEMPLATE;

  @Nullable
  public static Credentials getCredentials(BigQueryConnectorConfig config) throws IOException {
    return config.getServiceAccount() == null ?
      null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(), config.isServiceAccountFilePath());
  }

  /**
   * Gets bucket from supplied configuration.
   *
   * If the supplied configuration doesn't specify a bucket, a bucket will get auto created and configuration modified
   * to auto-delete this bucket on completion.
   *
   * @param configuration Hadoop configuration instance.
   * @param storage GCS Storage client
   * @param bucket GCS bucket
   * @param dataset BigQuery dataset
   * @param bucketPath bucket path to use. Will be used as a bucket name if needed..
   * @param cmekKeyName CMEK key to use for the auto-created bucket.
   * @return Bucket name.
   */
  public static String getOrCreateBucket(Configuration configuration,
                                         Storage storage,
                                         @Nullable String bucket,
                                         Dataset dataset,
                                         String bucketPath,
                                         @Nullable CryptoKeyName cmekKeyName) throws IOException {
    // Create a new bucket if needed
    if (bucket == null) {
      bucket = String.format(BQ_TEMP_BUCKET_NAME_TEMPLATE, bucketPath);
      // By default, this option is false, meaning the job can not delete the bucket. So enable it only when bucket name
      // is not provided.
      configuration.setBoolean("fs.gs.bucket.delete.enable", true);
      GCPUtils.createBucket(storage, bucket, dataset.getLocation(), cmekKeyName);
    } else if (storage != null && storage.get(bucket) == null) {
      try {
        GCPUtils.createBucket(storage, bucket, dataset.getLocation(), cmekKeyName);
      } catch (StorageException e) {
        if (e.getCode() == 409) {
          // A conflict means the bucket already exists
          // This most likely means multiple stages in the same pipeline are trying to create the same bucket.
          // Ignore this and move on, since all that matters is that the bucket exists.
          return bucket;
        }
        throw new IOException(String.format("Unable to create Cloud Storage bucket '%s' in the same " +
                                              "location ('%s') as BigQuery dataset '%s'. " + "Please use a bucket " +
                                              "that is in the same location as the dataset.",
                                            bucket, dataset.getLocation(), dataset.getDatasetId().getDataset()), e);
      }
    }

    return bucket;
  }

  /**
   * Sets up service account credentials into supplied Hadoop configuration.
   *
   * @param configuration Hadoop Configuration instance.
   * @param config BigQuery connection configuration.
   */
  public static void configureServiceAccount(Configuration configuration, BigQueryConnectorConfig config) {
    if (config.getServiceAccount() != null) {
      configuration.set(BigQueryConstants.CONFIG_SERVICE_ACCOUNT, config.getServiceAccount());
      configuration.setBoolean(BigQueryConstants.CONFIG_SERVICE_ACCOUNT_IS_FILE, config.isServiceAccountFilePath());
    }
  }

  /**
   * Configure BigQuery input using the supplied configuration and GCS path.
   *
   * @param configuration Hadoop configuration instance.
   * @param dataset the dataset to use.
   * @param table the name of the table to pull from.
   * @param gcsPath Path to use to store output files.
   * @throws IOException if the BigQuery input could not be configured.
   */
  public static void configureBigQueryInput(Configuration configuration,
                                            DatasetId dataset,
                                            String table,
                                            String gcsPath) throws IOException {
    // Configure GCS bucket path
    LOG.debug("Using GCS path {} as temp storage for table {}.", gcsPath, table);
    configuration.set("fs.default.name", gcsPath);
    configuration.setBoolean("fs.gs.impl.disable.cache", true);
    configuration.setBoolean("fs.gs.metadata.cache.enable", false);

    // Set up temporary table name. This will be used if the source table is a view
    String temporaryTableName = String.format("_%s_%s", table,
                                              UUID.randomUUID().toString().replaceAll("-", "_"));
    configuration.set(BigQueryConstants.CONFIG_TEMPORARY_TABLE_NAME, temporaryTableName);

    // Configure BigQuery input format.
    PartitionedBigQueryInputFormat.setTemporaryCloudStorageDirectory(configuration,
                                                                     gcsPath);
    BigQueryConfiguration.configureBigQueryInput(configuration,
                                                 dataset.getProject(),
                                                 dataset.getDataset(),
                                                 table);
  }

  /**
   * Build GCS path for a supplied bucket, prefix and table name.
   *
   * @param bucket bucket name.
   * @param pathPrefix path prefix.
   * @param tableName table name.
   * @return formatted GCS path.
   */
  public static String getTemporaryGcsPath(String bucket, String pathPrefix, String tableName) {
    return String.format(TEMPORARY_BUCKET_FORMAT, bucket, pathPrefix, tableName);
  }

  /**
   * Deletes temporary BigQuery table used to export records from views.
   *
   * @param configuration Hadoop Configuration.
   * @param config BigQuery source configuration.
   */
  public static void deleteBigQueryTemporaryTable(Configuration configuration, BigQuerySourceConfig config) {
    String temporaryTable = configuration.get(BigQueryConstants.CONFIG_TEMPORARY_TABLE_NAME);
    try {
      Credentials credentials = getCredentials(config.getConnection());
      BigQuery bigQuery = GCPUtils.getBigQuery(config.getProject(), credentials);
      bigQuery.delete(TableId.of(config.getDatasetProject(), config.getDataset(), temporaryTable));
      LOG.debug("Deleted temporary table '{}'", temporaryTable);
    } catch (IOException e) {
      LOG.error("Failed to load service account credentials: {}", e.getMessage(), e);
    }
  }

  /**
   * Deletes temporary GCS directory.
   *
   * @param configuration Hadoop Configuration.
   * @param bucket the bucket name
   * @param runId the run ID
   */
  public static void deleteGcsTemporaryDirectory(Configuration configuration,
                                                 String bucket,
                                                 String runId) {
    String gcsPath;
    // If the bucket was created for this run, build temp path name using the bucket path and delete the entire bucket.
    if (bucket == null) {
      gcsPath = String.format(BQ_TEMP_BUCKET_PATH_TEMPLATE, runId);
    } else {
      gcsPath = String.format(GS_PATH_FORMAT, bucket, runId);
    }

    try {
      BigQueryUtil.deleteTemporaryDirectory(configuration, gcsPath);
    } catch (IOException e) {
      LOG.error("Failed to delete temporary directory '{}': {}", gcsPath, e.getMessage());
    }
  }
}
