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
import co.cask.gcp.common.FileSetUtil;
import co.cask.gcp.common.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import parquet.avro.AvroParquetOutputFormat;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link GCSParquetBatchSink} that stores data in parquet format to Google Cloud Storage.
 */
@Plugin(type = "batchsink")
@Name("GCSParquet")
@Description("Writes records to one or more parquet files in a directory on Google Cloud Storage.")
public class GCSParquetBatchSink extends GCSBatchSink<Void, GenericRecord> {

  private StructuredToAvroTransformer recordTransformer;
  private final GCSParquetSinkConfig config;
  private static final String PARQUET_AVRO_SCHEMA = "parquet.avro.schema";

  public GCSParquetBatchSink(GCSParquetSinkConfig config) {
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
                        Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(null, recordTransformer.transform(input)));
  }

  @Override
  protected String getOutputFormatClassname() {
    return AvroParquetOutputFormat.class.getName();
  }

  @Override
  protected Map<String, String> getOutputFormatConfig() {
    Map<String, String> conf = new HashMap<>();
    conf.put(PARQUET_AVRO_SCHEMA, config.schema);
    conf.putAll(FileSetUtil.getParquetCompressionConfiguration(config.codec, config.schema, true));
    return conf;
  }

  /**
   * Configuration for the sink.
   */
  public static class GCSParquetSinkConfig extends GCSBatchSinkConfig {
    @Name("schema")
    @Description("The schema of records to write.")
    @Macro
    protected String schema;

    @Nullable
    @Description("The compression codec to use when writing data. Must be 'none', 'snappy', 'gzip', or 'lzo'.")
    private String codec;
  }
}
