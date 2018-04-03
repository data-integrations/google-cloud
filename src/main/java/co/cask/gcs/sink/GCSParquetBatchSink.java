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
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.common.FileSetUtil;
import co.cask.common.StructuredToAvroTransformer;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.text.SimpleDateFormat;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link GCSParquetBatchSink} that stores data in parquet format to S3.
 */
@Plugin(type = "batchsink")
@Name("GCSParquet")
@Description("GCS Parquet Batch Sink.")
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
  protected OutputFormatProvider createOutputFormatProvider(BatchSinkContext context) {
    return new GCSParquetOutputFormatProvider(config, context);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<Void, GenericRecord>(null, recordTransformer.transform(input)));
  }

  /**
   * Configuration for the S3ParquetConfig.
   */
  public static class GCSParquetSinkConfig extends GCSBatchSinkConfig {
    @Name("schema")
    @Description("Specifies the schema to used for defining the GCS Dataset.")
    @Macro
    protected String schema;

    @Nullable
    @Description("Used to specify the compression codec to be used for the final dataset.")
    private String codec;

    @SuppressWarnings("unused")
    public GCSParquetSinkConfig() {
      super();
    }

    @SuppressWarnings("unused")
    public GCSParquetSinkConfig(String referenceName, String path,
                                String format, String properties, @Nullable String codec,
                                String project, String serviceAccountFilePath, String bucket) {
      super(referenceName, path, format, properties, project, serviceAccountFilePath, bucket);
      this.codec = codec;
    }
  }

  /**
   * Output format provider that sets parquet output format to be use in MapReduce.
   */
  public static class GCSParquetOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    public GCSParquetOutputFormatProvider(GCSParquetSinkConfig config, BatchSinkContext context) {
      @SuppressWarnings("ConstantConditions")
      SimpleDateFormat format = new SimpleDateFormat(config.format);
      conf = Maps.newHashMap();
      conf.put(PARQUET_AVRO_SCHEMA, config.schema);
      conf.putAll(FileSetUtil.getParquetCompressionConfiguration(config.codec, config.schema, true));
      conf.put(FileOutputFormat.OUTDIR,
               String.format("%s/%s", config.path, format.format(context.getLogicalStartTime())));
    }

    @Override
    public String getOutputFormatClassName() {
      return AvroParquetOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
