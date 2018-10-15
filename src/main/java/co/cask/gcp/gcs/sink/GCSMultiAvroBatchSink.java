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

package co.cask.gcp.gcs.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.gcp.common.RecordFilterOutputFormat;
import org.apache.hadoop.io.NullWritable;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link GCSMultiAvroBatchSink} that stores data in avro format to Google Cloud Storage.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCSMultiAvro")
@Description("Writes records to one or more avro files in a directory on Google Cloud Storage.")
public class GCSMultiAvroBatchSink extends GCSMultiBatchSink<NullWritable, StructuredRecord> {

  private final GCSMultiAvroSinkConfig config;

  public GCSMultiAvroBatchSink(GCSMultiAvroSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
//    recordTransformer = new RecordFilterOutputFormat(config.schema);
  }




  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
  }

  @Override
  protected String getOutputFormatClassname() {
    return RecordFilterOutputFormat.class.getName();
  }

  @Override
  protected Map<String, String> getOutputFormatConfig() {
    Map<String, String> conf = new HashMap<>();
//    conf.putAll(FileSetUtil.getAvroCompressionConfiguration(config.codec, config.schema, false));
//    conf.put(JobContext.OUTPUT_KEY_CLASS, AvroKey.class.getName());
    conf.put(RecordFilterOutputFormat.FILTER_FIELD, config.splitField);
    return conf;
  }

  /**
   * Configuration for the GCSAvroSink.
   */
  public static class GCSMultiAvroSinkConfig extends GCSBatchSinkConfig {
    @Name("schema")
    @Description("The schema of records to write.")
    @Macro
    @Nullable
    protected String schema;

    @Name("codec")
    @Description("The compression codec to use when writing data. Must be 'none', 'snappy', or 'deflated'.")
    @Nullable
    private String codec;

    @Nullable
    @Description("The name of the field that will be used to determine which fileset to write to. " +
            "Defaults to 'tablename'.")
    private String splitField;

    @Nullable
    @Description("The delimiter to use to separate record fields. Defaults to the tab character.")
    private String delimiter;

  }
}
