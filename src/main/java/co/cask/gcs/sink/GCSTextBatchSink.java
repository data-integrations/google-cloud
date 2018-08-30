/*
 * Copyright © 2015, 2016 Cask Data, Inc.
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

package co.cask.gcs.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.cloud.ServiceOptions;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link GCSTextBatchSink} that stores data in avro format to S3.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCSText")
@Description("GCS Text Batch Sink.")
public class GCSTextBatchSink extends GCSBatchSink<NullWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(GCSTextBatchSink.class);
  private final GCSTextSinkConfig config;
  private Convertor convertor;

  public GCSTextBatchSink(GCSTextSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    convertor = new Json();
    if (config.type.equalsIgnoreCase("Text Comma Separated")) {
      convertor = new CommaSeparated();
    } else if (config.type.equalsIgnoreCase("Text Tab Separated")) {
      convertor = new TabSeparated();
    } else if (config.type.equalsIgnoreCase("Text Pipe Separated")) {
      convertor = new PipeSeparated();
    } else if (config.type.equalsIgnoreCase("Text Ctrl+A Separated")) {
      convertor = new CtrlASeparated();
    }
  }

  @Override
  protected OutputFormatProvider createOutputFormatProvider(BatchSinkContext context) {
    return new GCSTextOutputFormatProvider(config, context);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    try {
      String record = convertor.convert(input);
      emitter.emit(new KeyValue<>(NullWritable.get(), new Text(record)));
    } catch (IOException e) {
      // no-op
    }
  }

  interface Convertor {
    String convert(StructuredRecord input) throws IOException;
  }

  private static class CommaSeparated implements Convertor {
    @Override
    public String convert(StructuredRecord input) throws IOException {
      return StructuredRecordStringConverter.toDelimitedString(input, ",");
    }
  }

  private static class TabSeparated implements Convertor {
    @Override
    public String convert(StructuredRecord input) throws IOException {
      return StructuredRecordStringConverter.toDelimitedString(input, "\t");
    }
  }

  private static class PipeSeparated implements Convertor {
    @Override
    public String convert(StructuredRecord input) throws IOException {
      return StructuredRecordStringConverter.toDelimitedString(input, "|");
    }
  }

  private static class Json implements Convertor {
    @Override
    public String convert(StructuredRecord input) throws IOException {
      return StructuredRecordStringConverter.toJsonString(input);
    }
  }

  private static class CtrlASeparated implements Convertor {
    @Override
    public String convert(StructuredRecord input) throws IOException {
      return StructuredRecordStringConverter.toDelimitedString(input, "\u0001");
    }
  }

  /**
   * Configuration for the GCSAvroSink.
   */
  public static class GCSTextSinkConfig extends GCSBatchSinkConfig {
    @Name("type")
    @Description("Specifies the type of output.")
    private String type;

    @SuppressWarnings("unused")
    public GCSTextSinkConfig() {
      super();
    }

    @SuppressWarnings("unused")
    public GCSTextSinkConfig(String referenceName, String path,
                             String format, String properties, String type,
                             String project, String serviceAccountFilePath, String bucket) {
      super(referenceName, path, format, properties, project, serviceAccountFilePath, bucket);
      this.type = type;
    }
  }

  /**
   * Output format provider that sets avro output format to be use in MapReduce.
   */
  public static class GCSTextOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> properties = new HashMap<>();

    public GCSTextOutputFormatProvider(GCSTextSinkConfig config, BatchSinkContext context) {
      SimpleDateFormat format = new SimpleDateFormat(config.format);
      properties.put(JobContext.OUTPUT_KEY_CLASS, Text.class.getName());
      properties.put(FileOutputFormat.OUTDIR, String.format("%s/%s", config.path,
                                                      format.format(context.getLogicalStartTime())));
      if (config.serviceAccountFilePath != null) {
        properties.put("mapred.bq.auth.service.account.json.keyfile", config.serviceAccountFilePath);
        properties.put("google.cloud.auth.service.account.json.keyfile", config.serviceAccountFilePath);
      }
      properties.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
      properties.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
      String projectId = config.project == null ? ServiceOptions.getDefaultProjectId() : config.project;
      properties.put("fs.gs.project.id", projectId);
      properties.put("fs.gs.system.bucket", config.bucket);
      properties.put("fs.gs.impl.disable.cache", "true");
    }

    @Override
    public String getOutputFormatClassName() {
      return TextOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return properties;
    }
  }
}
