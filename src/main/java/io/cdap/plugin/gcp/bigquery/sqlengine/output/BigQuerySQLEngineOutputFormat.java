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

package io.cdap.plugin.gcp.bigquery.sqlengine.output;

import com.google.cloud.hadoop.io.bigquery.BigQueryFactory;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.ForwardingBigQueryFileOutputFormat;
import io.cdap.plugin.gcp.bigquery.sink.BigQueryOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * An Output Format which extends {@link BigQueryOutputFormat}.
 *
 * This implementation overrides the implementation of
 * {@link ForwardingBigQueryFileOutputFormat#checkOutputSpecs(JobContext)} in order to skip a check that prevents
 * compression from being used.
 */
public class BigQuerySQLEngineOutputFormat extends BigQueryOutputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySQLEngineOutputFormat.class);

  /**
   * Copy of {@link ForwardingBigQueryFileOutputFormat#checkOutputSpecs} which skips check for compression
   * @param job the job context
   * @throws FileAlreadyExistsException if the file already exists
   * @throws IOException
   */
  @Override
  public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
    Configuration conf = job.getConfiguration();

    // Validate the output configuration.
    BigQueryOutputConfiguration.validateConfiguration(conf);

    // Get the output path.
    Path outputPath = BigQueryOutputConfiguration.getGcsOutputPath(conf);
    LOG.info("Using output path '{}'.", outputPath);

    // Error if the output path already exists.
    FileSystem outputFileSystem = outputPath.getFileSystem(conf);
    if (outputFileSystem.exists(outputPath)) {
      throw new IOException("The output path '" + outputPath + "' already exists.");
    }

    // Error if unable to create a BigQuery helper.
    try {
      new BigQueryFactory().getBigQueryHelper(conf);
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create BigQuery client", gse);
    }

    // Let delegate process its checks.
    getDelegate(conf).checkOutputSpecs(job);
  }
}
