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
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.common.FileSetUtil;
import co.cask.common.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobContext;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link GCSAvroBatchSink} that stores data in avro format to Google Cloud Storage.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCSAvro")
@Description("Writes records to one or more avro files in a directory on Google Cloud Storage.")
public class GCSAvroBatchSink extends GCSBatchSink<AvroKey<GenericRecord>, NullWritable> {
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
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }

  @Override
  protected String getOutputFormatClassname() {
    return AvroKeyOutputFormat.class.getName();
  }

  @Override
  protected Map<String, String> getOutputFormatConfig() {
    Map<String, String> conf = new HashMap<>();
    conf.putAll(FileSetUtil.getAvroCompressionConfiguration(config.codec, config.schema, false));
    conf.put(JobContext.OUTPUT_KEY_CLASS, AvroKey.class.getName());
    return conf;
  }

  /**
   * Configuration for the GCSAvroSink.
   */
  public static class GCSAvroSinkConfig extends GCSBatchSinkConfig {
    @Name("schema")
    @Description("The schema of records to write.")
    @Macro
    protected String schema;

    @Name("codec")
    @Description("The compression codec to use when writing data. Must be 'none', 'snappy', or 'deflated'.")
    @Nullable
    private String codec;
  }
}
