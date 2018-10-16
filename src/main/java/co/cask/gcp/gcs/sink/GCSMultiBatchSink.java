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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.gcp.common.*;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.annotation.Nullable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * {@link GCSMultiBatchSink} that stores the data of the latest run of an adapter in S3.
 *
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class GCSMultiBatchSink<KEY_OUT, VAL_OUT> extends ReferenceSink<StructuredRecord, KEY_OUT, VAL_OUT> {
    public static final String TABLE_PREFIX = "multisink.";
    private static final String PARQUET_AVRO_SCHEMA = "parquet.avro.schema";

    private final GCSBatchSinkConfig config;

    public GCSMultiBatchSink(GCSBatchSinkConfig config) {
        super(config);
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
            String schema = argument.getValue();
            String dbTableName = key.substring(TABLE_PREFIX.length());
            //dbTableName is of the form db:table
            String[] parts = dbTableName.split(":");
            String db = parts[0];
            String name = parts[1];

            Job job = JobUtils.createInstance();
            Configuration outputConfig = job.getConfiguration();

            outputConfig.set(FileOutputFormat.OUTDIR, String.format("%s_%s_%s", config.getOutputDir(context.getLogicalStartTime()), db, name));
            if (config.serviceFilePath != null) {
                outputConfig.set("mapred.bq.auth.service.account.json.keyfile", config.serviceFilePath);
                outputConfig.set("google.cloud.auth.service.account.json.keyfile", config.serviceFilePath);
            }
            outputConfig.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
            outputConfig.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
            String projectId = GCPUtils.getProjectId(config.project);
            outputConfig.set("fs.gs.project.id", projectId);
            outputConfig.set("fs.gs.system.bucket", config.bucket);
            outputConfig.set("fs.gs.impl.disable.cache", "true");

            outputConfig.set(RecordFilterOutputFormat.PASS_VALUE, name);
            outputConfig.set(RecordFilterOutputFormat.ORIGINAL_SCHEMA, schema);
            outputConfig.set(RecordFilterOutputFormat.FORMAT, config.outputFormat);


            Map<String, String> outputFormatConfig = getOutputFormatConfig();
            for (Map.Entry<String, String> entry : outputFormatConfig.entrySet()) {
                outputConfig.set(entry.getKey(), entry.getValue());
            }

            job.setOutputValueClass(NullWritable.class);
            if (RecordFilterOutputFormat.AVRO.equals(config.outputFormat)) {
                org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema);
                for (Map.Entry<String, String> entry : FileSetUtil.getAvroCompressionConfiguration(config.codec, config.schema, false).entrySet()) {
                    outputConfig.set(entry.getKey(), entry.getValue());
                }
                AvroJob.setOutputKeySchema(job, avroSchema);
            } else if (RecordFilterOutputFormat.ORC.equals(config.outputFormat)) {
                StringBuilder builder = new StringBuilder();
                co.cask.hydrator.common.HiveSchemaConverter.appendType(builder, Schema.parseJson(schema));
                outputConfig.set("orc.mapred.output.schema", builder.toString());
            } else if (RecordFilterOutputFormat.PARQUET.equals(config.outputFormat)) {
                for (Map.Entry<String, String> entry : FileSetUtil.getParquetCompressionConfiguration(config.codec, config.schema, false).entrySet()) {
                    outputConfig.set(entry.getKey(), entry.getValue());
                }
                outputConfig.set(PARQUET_AVRO_SCHEMA,schema);
            }  else {
                // Encode the delimiter to base64 to support control characters. Otherwise serializing it in Cconf would result
                // in an error
                outputConfig.set(RecordFilterOutputFormat.DELIMITER,config.delimiter);
            }


            context.addOutput(Output.of(config.referenceName,
                    new SinkOutputFormatProvider(getOutputFormatClassname(), outputConfig)));
        }
    }

    protected abstract String getOutputFormatClassname();

    protected abstract Map<String, String> getOutputFormatConfig();

    @VisibleForTesting
    GCSBatchSinkConfig getConfig() {
        return config;
    }

    /**
     * Sink configuration.
     */
    public static class GCSBatchSinkConfig extends ReferenceConfig {
        @Name("path")
        @Description("The path to write to. For example, gs://<bucket>/path/to/directory")
        @Macro
        protected String path;

        @Description("The time format for the output directory that will be appended to the path. " +
                "For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. " +
                "If not specified, nothing will be appended to the path.")
        @Nullable
        @Macro
        protected String suffix;

        @Description(GCPConfig.PROJECT_DESC)
        @Macro
        @Nullable
        protected String project;

        @Description(GCPConfig.SERVICE_ACCOUNT_DESC)
        @Macro
        @Nullable
        protected String serviceFilePath;

        @Description("Name of the bucket.")
        @Macro
        protected String bucket;


        @Description("The format of output files.")
        @Macro
        private String outputFormat;

        @Nullable
        @Description("The delimiter to use to separate record fields. Defaults to the tab character.")
        private String delimiter;

        @Name("schema")
        @Description("The schema of records to write.")
        @Macro
        @Nullable
        protected String schema;

        @Name("codec")
        @Description("The compression codec to use when writing data. Must be 'none', 'snappy', or 'deflated'.")
        @Nullable
        private String codec;


        public GCSBatchSinkConfig() {
            // Set default value for Nullable properties.
            super("");
        }

        public void validate() {
            if (path != null && !containsMacro("path") && !path.startsWith("gs://")) {
                throw new IllegalArgumentException("Path must start with gs://.");
            }
            if (suffix != null && !containsMacro("suffix")) {
                new SimpleDateFormat(suffix);
            }
        }

        protected String getOutputDir(long logicalStartTime) {
            String timeSuffix = !Strings.isNullOrEmpty(suffix) ? new SimpleDateFormat(suffix).format(logicalStartTime) : "";
            return String.format("%s/%s", path, timeSuffix);
        }
    }
}
