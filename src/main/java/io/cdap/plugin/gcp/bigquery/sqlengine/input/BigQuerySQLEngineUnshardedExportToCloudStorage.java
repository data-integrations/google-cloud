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

package io.cdap.plugin.gcp.bigquery.sqlengine.input;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings;
import com.google.cloud.hadoop.io.bigquery.ExportFileFormat;
import com.google.cloud.hadoop.io.bigquery.UnshardedExportToCloudStorage;
import io.cdap.plugin.gcp.bigquery.sqlengine.util.BigQuerySQLEngineUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Export which overrides {@link UnshardedExportToCloudStorage} in order to add Snappy compression to Avro exports.
 */
public class BigQuerySQLEngineUnshardedExportToCloudStorage extends UnshardedExportToCloudStorage {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySQLEngineUnshardedExportToCloudStorage.class);

  public BigQuerySQLEngineUnshardedExportToCloudStorage(Configuration configuration,
                                                        String gcsPath,
                                                        ExportFileFormat fileFormat,
                                                        BigQueryHelper bigQueryHelper,
                                                        String projectId, Table tableToExport) {
    super(configuration, gcsPath, fileFormat, bigQueryHelper, projectId, tableToExport, null);
  }

  /**
   * Copy of {@link UnshardedExportToCloudStorage#beginExport()} with logic added to enable Snappy compression.
   */
  @Override
  public void beginExport() throws IOException {
    // Create job and configuration.
    JobConfigurationExtract extractConfig = new JobConfigurationExtract();

    // Set source.
    extractConfig.setSourceTable(tableToExport.getTableReference());

    // Set destination.
    extractConfig.setDestinationUris(getExportPaths());
    extractConfig.set(DESTINATION_FORMAT_KEY, fileFormat.getFormatIdentifier());

    // Set compression for Avro exports
    if (fileFormat == ExportFileFormat.AVRO) {
      extractConfig.setCompression(BigQuerySQLEngineUtils.CODEC_SNAPPY);
    }

    JobConfiguration config = new JobConfiguration();
    config.setExtract(extractConfig);

    JobReference jobReference =
      bigQueryHelper.createJobReference(
        projectId, "exporttocloudstorage", tableToExport.getLocation());

    Job job = new Job();
    job.setConfiguration(config);
    job.setJobReference(jobReference);

    // Insert and run job.
    try {
      Job response = bigQueryHelper.insertJobOrFetchDuplicate(projectId, job);
      LOG.debug("Got response '{}'", response);
      exportJobReference = response.getJobReference();
    } catch (IOException e) {
      String error = String.format(
        "Error while exporting table %s",
        BigQueryStrings.toString(tableToExport.getTableReference()));
      throw new IOException(error, e);
    }
  }
}
