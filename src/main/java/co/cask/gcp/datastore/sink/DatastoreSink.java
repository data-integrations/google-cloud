/*
 * Copyright © 2019 Cask Data, Inc.
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
package co.cask.gcp.datastore.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.LineageRecorder;
import com.google.cloud.datastore.FullEntity;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * A {@link BatchSink} that writes data to Cloud Datastore.
 * This {@link DatastoreSink} takes a {@link StructuredRecord} in, converts it to Entity, and writes it to the
 * Cloud Datastore kind.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(DatastoreSink.PLUGIN_NAME)
@Description("CDAP Google Cloud Datastore Batch Sink takes the structured record from the input source and writes "
  + "to Google Cloud Datastore.")
public class DatastoreSink extends BatchSink<StructuredRecord, NullWritable, FullEntity<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreSink.class);

  public static final String PLUGIN_NAME = "Datastore";

  private final DatastoreSinkConfig config;
  private final DatastoreSinkTransformer datastoreSinkTransformer;

  public DatastoreSink(DatastoreSinkConfig config) {
    this.config = config;
    this.datastoreSinkTransformer = new DatastoreSinkTransformer(config);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(inputSchema);
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void prepareRun(BatchSinkContext context) {
    Schema inputSchema = context.getInputSchema();
    LOG.debug("DatastoreSink `prepareRun` input schema: {}", inputSchema);
    config.validate(inputSchema);

    context.addOutput(Output.of(config.getReferenceName(), new DatastoreOutputFormatProvider(config)));

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(inputSchema);
    // Record the field level WriteOperation
    lineageRecorder.recordWrite("Write", "Wrote to Cloud Datastore sink",
                                inputSchema.getFields().stream()
                                  .map(Schema.Field::getName)
                                  .collect(Collectors.toList()));
  }

  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<NullWritable, FullEntity<?>>> emitter) {
    FullEntity<?> fullEntity = datastoreSinkTransformer.transformStructuredRecord(record);
    emitter.emit(new KeyValue<>(null, fullEntity));
  }
}
