/*
 * Copyright © 2018 Cask Data, Inc.
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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.gcp.common.FileSetUtil;
import co.cask.gcp.common.GCPConfig;
import co.cask.gcp.common.GCPUtils;
//import co.cask.gcp.common.RecordFilterOutputFormat;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;


/**
 * Writes to multiple partitioned file sets.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("DynamicMultiGCS")
@Description("Writes to multiple partitioned file sets. File sets are partitioned by an ingesttime field " +
        "that will be set to the logical start time of the pipeline run. The sink will write to the correct sink based " +
        "on the value of a split field. For example, if the split field is configured to be 'tablename', any record " +
        "with a 'tablename' field of 'xyz' will be written to file set 'xyz'. This plugin expects that the filesets " +
        "to write to will be present in the pipeline arguments. Each table to write to must have an argument where " +
        "the key is 'multisink.[name]' and the value is the schema for that fileset. Most of the time, " +
        "this plugin will be used with the MultiTableDatabase source, which will set those pipeline arguments.")
public class DynamicMultiGCSSink2 extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
    public static final String TABLE_PREFIX = "multisink.";



    public static final String FILTER_FIELD = "record.filter.field";
    public static final String PASS_VALUE = "record.filter.val";
    public static final String DELIMITER = "record.filter.delimiter";
    public static final String FORMAT = "record.output.format";
    public static final String ORIGINAL_SCHEMA = "record.original.schema";
    public static final String AVRO = "avro";
    public static final String TEXT = "text";
    public static final String ORC = "orc";

    private static final Gson GSON = new Gson();
    private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() {
    }.getType();

    private Conf config;

    public DynamicMultiGCSSink2(Conf config) {
        this.config = config;
    }


    @Override
    public void initialize(BatchRuntimeContext context) throws Exception {
        super.initialize(context);
    }

    @Override
    public void prepareRun(BatchSinkContext context) throws Exception {
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
            Configuration conf = job.getConfiguration();

//      conf.set(FileOutputFormat.OUTDIR, String.format("%s%s_%s%s",config.adlsBasePath, db, name, config.pathSuffix));

            conf.set(FileOutputFormat.OUTDIR, config.getOutputDir(context.getLogicalStartTime()));
            conf.set(FORMAT, config.outputFormat);
            conf.set(FILTER_FIELD, config.getSplitField());
            conf.set(PASS_VALUE, name);
            conf.set(ORIGINAL_SCHEMA, schema);
            Map<String, String> properties = config.getFileSystemProperties();

            if ("avro".equals(conf.get("record.output.format"))) {
                properties.putAll(config.getOutputFormatConfig());
            }
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }
            job.setOutputValueClass(NullWritable.class);
            if (AVRO.equals(config.outputFormat)) {
                org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema);
                AvroJob.setOutputKeySchema(job, avroSchema);
            } else if (ORC.equals(config.outputFormat)) {
                StringBuilder builder = new StringBuilder();
                co.cask.hydrator.common.HiveSchemaConverter.appendType(builder, Schema.parseJson(schema));
                conf.set("orc.mapred.output.schema", builder.toString());
            } else {
                // Encode the delimiter to base64 to support control characters. Otherwise serializing it in Cconf would result
                // in an error
                conf.set(DELIMITER,
                        Base64.encodeBase64String(Bytes.toBytesBinary(config.getFieldDelimiter())));
            }

//            context.addOutput(Output.of(name, new SinkOutputFormatProvider(RecordFilterOutputFormat.class.getName(), conf)));
        }
    }

    @Override
    public void transform(StructuredRecord input,
                          Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) throws Exception {
        emitter.emit(new KeyValue<>(NullWritable.get(), input));
    }

    /**
     * Plugin configuration properties.
     */
    public static class Conf extends PluginConfig {
        @Nullable
        @Macro
        @Description("Field delimiter for text format output files. Defaults to tab.")
        public String fieldDelimiter;
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
        @Name("schema")
        @Description("The schema of records to write.")
        @Macro
        protected String schema;
//
//    @Macro
//    @Description("ADLS base path to store.")
//    private String adlsBasePath;
//
//    @Macro
//    @Description("The suffix for ADLS base path. Used in conjunction with the ADLS basepath will produce an " +
//        "outputdirectory which is ADLSBasePath + TableName + suffix")
//    private String pathSuffix;
//
//    @Description("The Microsoft Azure Data Lake client id.")
//    @Macro
//    private String clientId;
//
//    @Description("The Microsoft Azure Data Lake refresh token URL.")
//    @Macro
//    private String refreshTokenURL;
//
//    @Description("The Microsoft Azure Data Lake credentials.")
//    @Macro
//    private String credentials;

//    @Nullable
//    @Description("A JSON string representing a map of properties needed for the distributed file system.")
//    @Macro
//    public String fileSystemProperties;
        @Name("codec")
        @Description("The compression codec to use when writing data. Must be 'none', 'snappy', or 'deflated'.")
        @Nullable
        protected String codec;

//    @Nullable
//    @Description("Output schema of the JSON document. Required for avro output format. " +
//        "If left empty for text output format, the schema of input records will be used." +
//        "This must be a subset of the schema of input records. " +
//        "Fields of type ARRAY, MAP, and RECORD are not supported with the text format. " +
//        "Fields of type UNION are only supported if they represent a nullable type.")
//    @Macro
//    public String schema;
        @Description("The format of output files.")
        @Macro
        private String outputFormat;
        @Nullable
        @Description("The name of the field that will be used to determine which file to write to. " +
                "Defaults to 'tablename'.")
        private String splitField;

        protected String getOutputDir(long logicalStartTime) {
            String timeSuffix = !Strings.isNullOrEmpty(suffix) ? new SimpleDateFormat(suffix).format(logicalStartTime) : "";
            return String.format("%s/%s", path, timeSuffix);
        }

        protected Map<String, String> getFileSystemProperties() {
            Map<String, String> properties = getProps();
//      properties.put("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem");
//      properties.put("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl");
//      properties.put("dfs.adls.oauth2.access.token.provider.type", "ClientCredential");
//      properties.put("dfs.adls.oauth2.refresh.url", refreshTokenURL);
//      properties.put("dfs.adls.oauth2.client.id", clientId);
//      properties.put("dfs.adls.oauth2.credential", credentials);


            if (serviceFilePath != null) {
                properties.put("mapred.bq.auth.service.account.json.keyfile", serviceFilePath);
                properties.put("google.cloud.auth.service.account.json.keyfile", serviceFilePath);
            }
            properties.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
            properties.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
            String projectId = GCPUtils.getProjectId(project);
            properties.put("fs.gs.project.id", projectId);
            properties.put("fs.gs.system.bucket", bucket);
            properties.put("fs.gs.impl.disable.cache", "true");

            return properties;
        }

        protected Map<String, String> getProps() {
            return new HashMap<>();
//      if (fileSystemProperties == null) {
//        return new HashMap<>();
//      }
//      try {
//        return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
//      } catch (Exception e) {
//        throw new IllegalArgumentException("Unable to parse fileSystemProperties: " + e.getMessage());
//      }
        }


        public String getSplitField() {
            return splitField == null ? "tablename" : splitField;
        }

        public String getFieldDelimiter() {
            return fieldDelimiter == null ? "\t" : fieldDelimiter;
        }

        protected Map<String, String> getOutputFormatConfig() {
            Map<String, String> conf = new HashMap<>();
            conf.putAll(FileSetUtil.getAvroCompressionConfiguration(codec, schema, false));
            conf.put(JobContext.OUTPUT_KEY_CLASS, AvroKey.class.getName());
            return conf;
        }
    }


}
