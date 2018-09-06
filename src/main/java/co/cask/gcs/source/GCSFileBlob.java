/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.gcs.source;


import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.common.GCPConfig;
import co.cask.common.ReferenceConfig;
import co.cask.common.WholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A source that uses the {@link WholeFileInputFormat} to read whole file as one record.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("GCSFileBlob")
@Description("Reads the entire content of a Google Cloud Storage object into a single record.")
public class GCSFileBlob extends BatchSource<String, BytesWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(GCSFileBlob.class);

  private final Config config;
  private final Schema outputSchema;

  public GCSFileBlob(Config config) {
    this.config = config;
    this.outputSchema = createOutputSchema();
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    configurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = createJob();

    Configuration configuration = job.getConfiguration();
    configuration.set("mapred.bq.auth.service.account.json.keyfile", config.serviceAccountFilePath);
    configuration.set("google.cloud.auth.service.account.json.keyfile", config.serviceAccountFilePath);
    configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configuration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    configuration.set("fs.gs.project.id", config.project);
    configuration.set("fs.gs.system.bucket", config.bucket);
    configuration.setBoolean("fs.gs.impl.disable.cache", true);

    FileInputFormat.setInputPaths(job, config.path);
    final String inputDir = configuration.get(FileInputFormat.INPUT_DIR);

    context.setInput(Input.of(config.referenceName, new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return WholeFileInputFormat.class.getName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(FileInputFormat.INPUT_DIR, inputDir);
        properties.put("mapred.bq.auth.service.account.json.keyfile", config.serviceAccountFilePath);
        properties.put("google.cloud.auth.service.account.json.keyfile", config.serviceAccountFilePath);
        properties.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        properties.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        properties.put("fs.gs.project.id", config.project);
        properties.put("fs.gs.system.bucket", config.bucket);
        properties.put("fs.gs.impl.disable.cache", "true");
        return properties;
      }
    }));
  }

  @Override
  public void transform(KeyValue<String, BytesWritable> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(StructuredRecord.builder(outputSchema)
                   .set("path", input.getKey())
                   .set("body", input.getValue().getBytes()).build());
  }

  private Schema createOutputSchema() {
    return Schema.recordOf("output", Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
                           Schema.Field.of("body", Schema.of(Schema.Type.BYTES)));
  }

  private static Job createJob() throws IOException {
    try {
      Job job = Job.getInstance();

      LOG.info("Job new instance");

      // some input formats require the credentials to be present in the job. We don't know for
      // sure which ones (HCatalog is one of them), so we simply always add them. This has no other
      // effect, because this method is only used at configure time and will be ignored later on.
      if (UserGroupInformation.isSecurityEnabled()) {
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        job.getCredentials().addAll(credentials);
      }

      return job;
    } catch (Exception e) {
      LOG.error("Exception ", e);
      throw e;
    }
  }

  /**
   * Configurations for the {@link GCSFileBlob} plugin.
   */
  public static final class Config extends ReferenceConfig {

    @Description("The path to read from. For example, gs://<bucket>/path/to/directory/")
    @Macro
    private String path;

    @Name("project")
    @Description(GCPConfig.PROJECT_DESC)
    @Macro
    private String project;

    @Name("serviceFilePath")
    @Description(GCPConfig.SERVICE_ACCOUNT_DESC)
    @Macro
    private String serviceAccountFilePath;

    @Name("bucket")
    @Description("Name of the bucket.")
    @Macro
    private String bucket;

    public Config(String referenceName) {
      super(referenceName);
    }
  }
}

