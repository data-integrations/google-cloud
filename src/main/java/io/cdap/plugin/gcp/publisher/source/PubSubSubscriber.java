/*
 * Copyright © 2020 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * Base implementation of a Realtime source plugin to read from Google PubSub.
 *
 * @param <T> The type that the PubSubMessage will be mapped to by the mapping function.
 */
public abstract class PubSubSubscriber<T> extends StreamingSource<T> {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubSubscriber.class);

  protected final PubSubSubscriberConfig config;
  protected final Schema schema;
  protected final SerializableFunction<PubSubMessage, T> mappingFunction;

  public PubSubSubscriber(PubSubSubscriberConfig conf, Schema schema,
                          SerializableFunction<PubSubMessage, T> mappingFunction) {
    this.config = conf;
    this.schema = schema;
    this.mappingFunction = mappingFunction;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(this.schema);
  }

  @Override
  public void prepareRun(StreamingSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    // record dataset lineage
    context.registerLineage(config.referenceName, this.schema);

    if (this.schema.getFields() != null) {
      LineageRecorder recorder = new LineageRecorder(context, config.referenceName);
      recorder.recordRead("Read", "Read from Pub/Sub",
                          this.schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public JavaDStream<T> getStream(StreamingContext context) throws Exception {
    return (JavaDStream<T>) PubSubSubscriberUtil.getStream(context, config)
      .map(pubSubMessage -> mappingFunction.apply(pubSubMessage));
  }

  @Override
  public int getRequiredExecutors() {
    return config.getNumberOfReaders();
  }

}
