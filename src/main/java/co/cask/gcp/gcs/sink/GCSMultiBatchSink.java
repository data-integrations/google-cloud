/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.gcp.gcs.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.gcp.common.FileSetUtil;
import co.cask.gcp.common.GCPUtils;
import co.cask.gcp.common.RecordFilterOutputFormat;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import co.cask.hydrator.format.FileFormat;
import com.google.common.base.Strings;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * {@link GCSMultiBatchSink} that stores the data of the latest run of an adapter in GCS.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCSMultiFiles")
@Description("Writes records to one or more avro files in a directory on Google Cloud Storage.")
public class GCSMultiBatchSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  private static final String TABLE_PREFIX = "multisink.";

  private final GCSMultiBatchSinkConfig config;

  public GCSMultiBatchSink(GCSMultiBatchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws IOException, UnsupportedTypeException {
    config.validate();
    for (Map.Entry<String, String> argument : context.getArguments()) {
      String key = argument.getKey();
      if (!key.startsWith(TABLE_PREFIX)) {
        continue;
      }
      String name = key.substring(TABLE_PREFIX.length());

      String schema = argument.getValue();

      Job job = JobUtils.createInstance();
      Configuration outputConfig = job.getConfiguration();

      outputConfig.set(FileOutputFormat.OUTDIR, config.getOutputDir(context.getLogicalStartTime(), name));
      Map<String, String> gcsProperties = GCPUtils.getFileSystemProperties(config);
      for (Map.Entry<String, String> entry : gcsProperties.entrySet()) {
        outputConfig.set(entry.getKey(), entry.getValue());
      }


      outputConfig.set(RecordFilterOutputFormat.PASS_VALUE, name);
      outputConfig.set(RecordFilterOutputFormat.ORIGINAL_SCHEMA, schema);
      String format = config.getOutputFormat();
      outputConfig.set(RecordFilterOutputFormat.FORMAT, format);


      outputConfig.set(RecordFilterOutputFormat.FILTER_FIELD, config.splitField);
      job.setOutputValueClass(org.apache.hadoop.io.NullWritable.class);
      if (RecordFilterOutputFormat.AVRO.equals(format)) {
        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema);
        Map<String, String> configuration =
            FileSetUtil.getAvroCompressionConfiguration(format, config.codec, schema, false);
        for (Map.Entry<String, String> entry : configuration.entrySet()) {
          outputConfig.set(entry.getKey(), entry.getValue());
        }
        AvroJob.setOutputKeySchema(job, avroSchema);
      } else if (RecordFilterOutputFormat.ORC.equals(format)) {
        StringBuilder builder = new StringBuilder();
        co.cask.hydrator.common.HiveSchemaConverter.appendType(builder, Schema.parseJson(schema));
        outputConfig.set("orc.mapred.output.schema", builder.toString());
      } else if (RecordFilterOutputFormat.PARQUET.equals(format)) {
        Map<String, String> configuration =
            FileSetUtil.getParquetCompressionConfiguration(format, config.codec, schema, false);
        for (Map.Entry<String, String> entry : configuration.entrySet()) {
          outputConfig.set(entry.getKey(), entry.getValue());
        }
      } else {
        // Encode the delimiter to base64 to support control characters. Otherwise serializing it in Cconf would result
        // in an error
        outputConfig.set(RecordFilterOutputFormat.DELIMITER, config.getDelimiter());
      }


      context.addOutput(
          Output.of(config.getReferenceName() + "_" + name,
              new SinkOutputFormatProvider(RecordFilterOutputFormat.class.getName(), outputConfig))
      );
    }
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
  }

  /**
   * Sink configuration.
   */
  public static class GCSMultiBatchSinkConfig extends GCSBatchSink.GCSBatchSinkConfig {

    @Name("codec")
    @Description("The codec to use when writing data. \n" +
        "The 'avro' format supports 'snappy' and 'deflate'. The parquet format supports 'snappy' and 'gzip'. \n" +
        "Other formats does not support compression.")
    @Nullable
    private String codec;

    @Description("The name of the field that will be used to determine which fileset to write to. " +
        "Defaults to 'tablename'.")
    private String splitField = "tablename";


    public GCSMultiBatchSinkConfig(String referenceName, String path, @Nullable String suffix, String format,
                                   @Nullable String delimiter, @Nullable String schema,
                                   @Nullable String codec, String splitField) {
      // Set default value for Nullable properties.
      super(referenceName, path, suffix, format, delimiter, schema);
      this.codec = codec;
      this.splitField = splitField == null ? "tablename" : splitField;

    }

    protected String getOutputDir(long logicalStartTime, String context) {
      boolean suffixOk = !Strings.isNullOrEmpty(getSuffix());
      String timeSuffix = suffixOk ? new SimpleDateFormat(getSuffix()).format(logicalStartTime) : "";
      return String.format("%s/%s/%s", getPath(), context, timeSuffix);
    }

    public String getOutputFormat() {
      return format;
    }

    @Override
    public FileFormat getFormat() {
      //Skip format validation to support ORC until the ORC will be supported by format-common
      //The values are from drop down box
      return RecordFilterOutputFormat.ORC.equals(format) ? null : super.getFormat();
    }

    @Override
    public void validate() {
      super.validate();
      if (RecordFilterOutputFormat.PARQUET.equalsIgnoreCase(format)) {
        FileSetUtil.isCompressionRequired(format, codec, FileSetUtil.PARQUET_CODECS);
      } else if (RecordFilterOutputFormat.AVRO.equalsIgnoreCase(format)) {
        FileSetUtil.isCompressionRequired(format, codec, FileSetUtil.AVRO_CODECS);
      } else if (!FileSetUtil.NONE.equalsIgnoreCase(codec)) {
        throw new IllegalArgumentException("format " + format + " does not support compression");
      }
    }
  }
}
