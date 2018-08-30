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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.common.FileSetUtil;
import co.cask.common.StructuredToAvroTransformer;
import com.google.cloud.ServiceOptions;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link GCSAvroBatchSink} that stores data in avro format to S3.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCSAvro")
@Description("GCS Avro Batch Sink.")
public class GCSAvroBatchSink extends GCSBatchSink<AvroKey<GenericRecord>, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(GCSAvroBatchSink.class);
  private StructuredToAvroTransformer recordTransformer;
  private final GCSAvroSinkConfig config;

  public GCSAvroBatchSink(GCSAvroSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema);
  }

  @Override
  protected OutputFormatProvider createOutputFormatProvider(BatchSinkContext context) {
    return new GCSAvroOutputFormatProvider(config, context);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }

  /**
   * Configuration for the GCSAvroSink.
   */
  public static class GCSAvroSinkConfig extends GCSBatchSinkConfig {
    @Name("schema")
    @Description("Specifies the schema to used for defining the GCS Dataset.")
    @Macro
    protected String schema;

    @Name("codec")
    @Description("Specifies compression codec to be used.")
    @Nullable
    private String codec;

    @SuppressWarnings("unused")
    public GCSAvroSinkConfig() {
      super();
    }

    @SuppressWarnings("unused")
    public GCSAvroSinkConfig(String referenceName, String path,
                             String format, String properties, @Nullable String codec,
                             String project, String serviceAccountFilePath, String bucket) {
      super(referenceName, path, format, properties, project, serviceAccountFilePath, bucket);
      this.codec = codec;
    }
  }

  /**
   * Output format provider that sets avro output format to be use in MapReduce.
   */
  public static class GCSAvroOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> properties = new HashMap<>();

    public GCSAvroOutputFormatProvider(GCSAvroSinkConfig config, BatchSinkContext context) {
      SimpleDateFormat format = new SimpleDateFormat(config.format);
      properties.putAll(FileSetUtil.getAvroCompressionConfiguration(config.codec, config.schema, false));
      properties.put(JobContext.OUTPUT_KEY_CLASS, AvroKey.class.getName());
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
      return AvroKeyOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return properties;
    }
  }
}
