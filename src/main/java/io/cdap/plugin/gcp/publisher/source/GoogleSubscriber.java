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
package io.cdap.plugin.gcp.publisher.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.cdap.etl.api.streaming.StreamingStateHandler;
import io.cdap.cdap.features.Feature;
import io.cdap.plugin.common.LineageRecorder;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Realtime source plugin to read from Google PubSub.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("GoogleSubscriber")
@Description("Streaming Source to read messages from Google PubSub.")
public class GoogleSubscriber extends PubSubSubscriber<StructuredRecord> implements StreamingStateHandler {

  private GoogleSubscriberConfig config;

  public GoogleSubscriber(GoogleSubscriberConfig config) {
    super(config);
    this.config = config;

    //Set mapping function for output records.
    super.setMappingFunction(RecordConverter.getFunction(config));
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    if (pipelineConfigurer.isFeatureEnabled(Feature.STREAMING_PIPELINE_NATIVE_STATE_TRACKING.getFeatureFlagString())) {
      // No retry at task and stage level. Jobs will be retried.
      Map<String, String> additionalProps = new HashMap<>();
      additionalProps.put("spark.task.maxFailures", "1");
      additionalProps.put("spark.stage.maxConsecutiveAttempts", "1");
      pipelineConfigurer.setPipelineProperties(additionalProps);
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void prepareRun(StreamingSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    Schema schema = context.getOutputSchema();
    // record dataset lineage
    context.registerLineage(config.referenceName, schema);

    if (schema.getFields() != null) {
      LineageRecorder recorder = new LineageRecorder(context, config.referenceName);
      recorder.recordRead("Read", "Read from Pub/Sub",
                          schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }
}
