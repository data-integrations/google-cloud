package io.cdap.plugin.gcp.dataplex.sink;

import com.google.common.collect.ImmutableMap;
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
  private final ValidatingOutputFormat delegate;
  private final Map<String, String> configMap;
  private final Configuration configuration;
  private final Schema tableSchema;

  public DataplexOutputFormatProvider(Configuration configuration, Schema tableSchema,
                                      ValidatingOutputFormat delegate, Map<String, String> configMap) {
  //for BQ assets
    this.configuration = configuration;
    this.tableSchema = tableSchema;
  //for GCS assets
    this.delegate = delegate;
    this.configMap = configMap == null ? null : ImmutableMap.copyOf(configMap);
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
    if (delegate == null) {
      Map<String, String> configToMap = BigQueryUtil.configToMap(configuration);
      if (tableSchema != null) {
        configToMap
          .put(BigQueryConstants.CDAP_BQ_SINK_OUTPUT_SCHEMA, tableSchema.toString());
      }
      return configToMap;
    }
    Map<String, String> outputFormatConfiguration = new HashMap<>(delegate.getOutputFormatConfiguration());
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

