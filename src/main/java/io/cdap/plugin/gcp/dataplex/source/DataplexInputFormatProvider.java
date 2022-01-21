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

package io.cdap.plugin.gcp.dataplex.source;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.plugin.common.batch.ConfigurationUtils;
import io.cdap.plugin.format.avro.input.CombineAvroInputFormat;
import io.cdap.plugin.gcp.bigquery.source.PartitionedBigQueryInputFormat;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexConstants;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * InputFormatProvider for DataplexSource
 */
public class DataplexInputFormatProvider implements InputFormatProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DataplexBatchSource.class);
  private static Configuration configuration;
  protected static Map<String, String> inputFormatConfiguration;

  /**
   * @param conf configuration
   */
  public DataplexInputFormatProvider(Configuration conf) {
    configuration = conf;
    if (conf != null) {
      String entityType = conf.get(DataplexConstants.DATAPLEX_ENTITY_TYPE);
      if (entityType.equalsIgnoreCase(DataplexConstants.BIGQUERY_DATASET_ENTITY_TYPE)) {
        inputFormatConfiguration = StreamSupport
          .stream(conf.spliterator(), false)
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      } else if (entityType.equalsIgnoreCase(DataplexConstants.STORAGE_BUCKET_ENTITY_TYPE)) {
        inputFormatConfiguration = ConfigurationUtils.getNonDefaultConfigurations(conf);
      }
    }
  }

  @Override
  public String getInputFormatClassName() {
    if (configuration != null) {
      String entityType = configuration.get(DataplexConstants.DATAPLEX_ENTITY_TYPE);
      if (entityType.equalsIgnoreCase(DataplexConstants.BIGQUERY_DATASET_ENTITY_TYPE)) {
        return PartitionedBigQueryInputFormat.class.getName();
      } else if (entityType.equalsIgnoreCase(DataplexConstants.STORAGE_BUCKET_ENTITY_TYPE)) {
        return DataplexInputFormat.class.getName();
      }
    }
    return null;
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return inputFormatConfiguration;
  }

  /**
   * InputFormat for DataplexSource
   */
  public static class DataplexInputFormat extends InputFormat<Object, Object> {
    private InputFormat delegateFormat;
    public DataplexInputFormat() {
      delegateFormat = new CombineAvroInputFormat();
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
      try {
        new DataplexUtil().getJobCompletion(jobContext.getConfiguration());
      } catch (Exception e) {
        LOG.error("Job failed in getSplits.");
        throw new IllegalStateException("Job creation failed in dataproc.", e);
      }
      return delegateFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<Object, Object> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
      return delegateFormat.createRecordReader(inputSplit, taskAttemptContext);
    }
  }
}
