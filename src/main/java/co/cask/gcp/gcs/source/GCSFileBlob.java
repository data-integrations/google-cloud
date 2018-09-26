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

package co.cask.gcp.gcs.source;


import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.api.lineage.field.FieldReadOperation;
import co.cask.gcp.common.GCPReferenceSourceConfig;
import co.cask.gcp.common.WholeFileInputFormat;
import co.cask.gcp.gcs.GCSConfigHelper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A source that uses the {@link WholeFileInputFormat} to read whole file as one record.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("GCSFileBlob")
@Description("Reads the entire content of a Google Cloud Storage object into a single record.")
public class GCSFileBlob extends BatchSource<String, BytesWritable, StructuredRecord> {
  private final Config config;
  private final Schema outputSchema;

  public GCSFileBlob(Config config) {
    this.config = config;
    this.outputSchema = createOutputSchema();
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    config.validate();
    configurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context){
    config.validate();
    context.setInput(Input.of(config.referenceName, new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return WholeFileInputFormat.class.getName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put(FileInputFormat.INPUT_DIR, GCSConfigHelper.getPath(config.path).toString());
        properties.put("google.cloud.auth.service.account.json.keyfile", config.getServiceAccountFilePath());
        properties.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        properties.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        properties.put("fs.gs.project.id", config.getProject());
        properties.put("fs.gs.system.bucket", GCSConfigHelper.getBucket(config.path));
        properties.put("fs.gs.impl.disable.cache", "true");
        return properties;
      }
    }));

    // record field level lineage information
    Schema outputSchema = context.getOutputSchema();
    if (outputSchema != null && outputSchema.getFields() != null && !outputSchema.getFields().isEmpty()) {
      FieldOperation operation = new FieldReadOperation("Read", "Read from Google Cloud Storage.",
                                                        EndPoint.of(context.getNamespace(), config.referenceName),
                                                        outputSchema.getFields().stream().map(Schema.Field::getName)
                                                          .collect(Collectors.toList()));
      context.record(Collections.singletonList(operation));
    }
  }

  @Override
  public void transform(KeyValue<String, BytesWritable> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(StructuredRecord.builder(outputSchema)
                   .set("path", input.getKey())
                   .set("body", input.getValue().getBytes()).build());
  }

  private Schema createOutputSchema() {
    return Schema.recordOf("output", Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
                           Schema.Field.of("body", Schema.of(Schema.Type.BYTES)));
  }

  /**
   * Configurations for the {@link GCSFileBlob} plugin.
   */
  public static final class Config extends GCPReferenceSourceConfig {

    @Description("The path to read from. For example, gs://<bucket>/path/to/directory/")
    @Macro
    private String path;

    void validate() {
      GCSConfigHelper.getPath(path);
    }
  }
}

