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

package io.cdap.plugin.gcp.dataplex.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.gcp.bigquery.sink.BigQueryOutputFormat;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.gcs.sink.GCSOutputFormatProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * OutputFormatProvider for Dataplex
 */
public class DataplexOutputFormatProvider implements ValidatingOutputFormat {
  public static final String RECORD_COUNT_FORMAT = "recordcount.%s";
  public static final String DATAPLEX_ASSET_TYPE = "dataplexsink.assettype";
  private static final String OUTPUT_FOLDER = "dataplexsink.metric.output.folder";
  private static final String DELEGATE_OUTPUTFORMAT_CLASSNAME = "gcssink.delegate.outputformat.classname";

  private final ValidatingOutputFormat delegate;
  private final Configuration configuration;
  private final Schema tableSchema;

  /**
   *
   * @param configuration it will be null for Asset type : Storage bucket
   * @param tableSchema it will be null for Asset type : Storage bucket
   * @param delegate it will be null for Asset type : BQ dataset
   */
  public DataplexOutputFormatProvider(Configuration configuration, Schema tableSchema,
                                      ValidatingOutputFormat delegate) {
  //for BQ assets
    this.configuration = configuration;
    this.tableSchema = tableSchema;
  //for GCS assets
    this.delegate = delegate;
  }


  @Override
  public void validate(FormatContext context) {
    delegate.validate(context);
  }

  @Override
  public String getOutputFormatClassName() {
    return DataplexOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    // delegate will be null in case of Bigquery Dataset asset.
    if (delegate == null) {
      Map<String, String> configToMap = BigQueryUtil.configToMap(configuration);
      if (tableSchema != null) {
        configToMap
          .put(BigQueryConstants.CDAP_BQ_SINK_OUTPUT_SCHEMA, tableSchema.toString());
      }
      return configToMap;
    }
    // It will run only for GCS asset
    Map<String, String> outputFormatConfiguration = new HashMap<>(delegate.getOutputFormatConfiguration());
    outputFormatConfiguration.put(DELEGATE_OUTPUTFORMAT_CLASSNAME, delegate.getOutputFormatClassName());
    return outputFormatConfiguration;
  }

  /**
   * OutputFormat for Dataplex Sink
   */
  public static class DataplexOutputFormat extends OutputFormat<Object, Object> {
    private final OutputFormat<NullWritable, StructuredRecord> gcsDelegateFormat =
      new GCSOutputFormatProvider.GCSOutputFormat();
    private final OutputFormat<StructuredRecord, NullWritable> bqDelegateFormat = new BigQueryOutputFormat();
    private OutputFormat delegateFormat;

    /**
     * It will set outputformat based on asset type in dataplex
     * @param configuration
     * @return
     * @throws IOException
     */
    private OutputFormat getDelegateFormatInstance(Configuration configuration) throws IOException {
      if (delegateFormat != null) {
        return delegateFormat;
      }
      String assetType = configuration.get(DATAPLEX_ASSET_TYPE);
      if (assetType.equalsIgnoreCase(DataplexBatchSink.BIGQUERY_DATASET_ASSET_TYPE)) {
        delegateFormat = bqDelegateFormat;
      } else {
        delegateFormat = gcsDelegateFormat;
      }
      return delegateFormat;
    }

    @Override
    public RecordWriter<Object, Object> getRecordWriter(TaskAttemptContext taskAttemptContext) throws
      IOException, InterruptedException {
      RecordWriter originalWriter = getDelegateFormatInstance(taskAttemptContext.getConfiguration())
        .getRecordWriter(taskAttemptContext);
      return new DataplexRecordWriter(originalWriter);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
      getDelegateFormatInstance(jobContext.getConfiguration()).checkOutputSpecs(jobContext);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException,
      InterruptedException {
      OutputCommitter delegateCommitter = getDelegateFormatInstance(taskAttemptContext.getConfiguration())
        .getOutputCommitter(taskAttemptContext);
      return new DataplexOutputCommitter(delegateCommitter);
    }
  }

  /**
   * RecordWriter for DataplexSink
   */
  public static class DataplexRecordWriter extends RecordWriter<Object, Object> {

    private final RecordWriter originalWriter;
    private long recordCount;

    public DataplexRecordWriter(RecordWriter originalWriter) {
      this.originalWriter = originalWriter;
    }

    @Override
    public void write(Object keyOut, Object valOut) throws IOException,
      InterruptedException {
      originalWriter.write(keyOut, valOut);
      recordCount++;
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      originalWriter.close(taskAttemptContext);
      //Since the file details are not available here, pass the value on in configuration
      taskAttemptContext.getConfiguration()
        .setLong(String.format(RECORD_COUNT_FORMAT, taskAttemptContext.getTaskAttemptID()), recordCount);
    }
  }
}

