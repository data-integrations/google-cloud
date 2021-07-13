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

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.io.bigquery.AvroBigQueryInputFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.Export;
import com.google.cloud.hadoop.io.bigquery.ExportFileFormat;
import com.google.cloud.hadoop.io.bigquery.NoopFederatedExportToCloudStorage;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.cloud.hadoop.util.HadoopToStringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;

import static com.google.common.flogger.LazyArgs.lazy;

/**
 * InputFormat which extends {@link AvroBigQueryInputFormat} and uses
 * {@link BigQuerySQLEngineUnshardedExportToCloudStorage} as the export logic, which allows us to use Snappy to
 * compress the output from BigQuery jobs.
 */
public class BigQuerySQLEngineAvroInputFormat extends AvroBigQueryInputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySQLEngineAvroInputFormat.class);

  /**
   * Copy of {@link AvroBigQueryInputFormat#getSplits(JobContext)} with a custom Export logic.
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    LOG.debug("getSplits({})", lazy(() -> HadoopToStringUtil.toString(context)));

    final Configuration configuration = context.getConfiguration();
    BigQueryHelper bigQueryHelper;
    try {
      bigQueryHelper = getBigQueryHelper(configuration);
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create BigQuery client", gse);
    }

    String exportPath =
      BigQueryConfiguration.getTemporaryPathRoot(configuration, context.getJobID());
    configuration.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, exportPath);

    Export export = constructExport(
      configuration,
      getExportFileFormat(),
      exportPath,
      bigQueryHelper);
    export.prepare();

    // Invoke the export, maybe wait for it to complete.
    try {
      export.beginExport();
      export.waitForUsableMapReduceInput();
    } catch (IOException ie) {
      throw new IOException("Error while exporting: " + HadoopToStringUtil.toString(context), ie);
    }

    List<InputSplit> splits = export.getSplits(context);

    if (LOG.isDebugEnabled()) {
      // Stringifying a really big list of splits can be expensive, so we guard with
      // isDebugEnabled().
      LOG.debug("getSplits -> {}", HadoopToStringUtil.toString(splits));
    }
    return splits;
  }

  /**
   * Copy of {@link AvroBigQueryInputFormat#constructExport} which uses
   * {@link BigQuerySQLEngineUnshardedExportToCloudStorage} as the export format.
   */
  private static Export constructExport(Configuration configuration,
                                        ExportFileFormat format,
                                        String exportPath,
                                        BigQueryHelper bigQueryHelper) throws IOException {
    LOG.debug("constructExport() with export path {}", exportPath);

    // Extract relevant configuration settings.
    Map<String, String> mandatoryConfig = ConfigurationUtil.getMandatoryConfig(
      configuration, BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_INPUT);
    String jobProjectId = mandatoryConfig.get(BigQueryConfiguration.PROJECT_ID_KEY);
    String inputProjectId = mandatoryConfig.get(BigQueryConfiguration.INPUT_PROJECT_ID_KEY);
    String datasetId = mandatoryConfig.get(BigQueryConfiguration.INPUT_DATASET_ID_KEY);
    String tableName = mandatoryConfig.get(BigQueryConfiguration.INPUT_TABLE_ID_KEY);

    TableReference exportTableReference = new TableReference()
      .setDatasetId(datasetId)
      .setProjectId(inputProjectId)
      .setTableId(tableName);
    Table table = bigQueryHelper.getTable(exportTableReference);

    if (EXTERNAL_TABLE_TYPE.equals(table.getType())) {
      LOG.info("Table is already external, so skipping export");
      return new NoopFederatedExportToCloudStorage(
        configuration, format, bigQueryHelper, jobProjectId, table, null);
    }

    return new BigQuerySQLEngineUnshardedExportToCloudStorage(
      configuration,
      exportPath,
      format,
      bigQueryHelper,
      jobProjectId,
      table);
  }
}
