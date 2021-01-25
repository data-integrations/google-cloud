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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation of a Realtime source plugin to read from Google PubSub.
 *
 * @param <T> The type that the PubSubMessage will be mapped to by the mapping function.
 */
public abstract class PubSubSubscriber<T> extends StreamingSource<T> {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubSubscriber.class);

  protected final PubSubSubscriberConfig config;
  protected final Schema recordSchema;
  protected final SerializableBiFunction<PubSubMessage, PubSubSubscriberConfig, T> mappingFunction;

  public PubSubSubscriber(PubSubSubscriberConfig conf, Schema recordSchema,
                          SerializableBiFunction<PubSubMessage, PubSubSubscriberConfig, T> mappingFunction) {
    this.config = conf;
    this.recordSchema = recordSchema;
    this.mappingFunction = mappingFunction;
  }

  @Override
  public JavaDStream<T> getStream(StreamingContext context) throws Exception {
    return (JavaDStream<T>) PubSubSubscriberUtil.getStream(context, config)
      .map(pubSubMessage -> mappingFunction.apply(pubSubMessage, config));
  }

  @Override
  public int getRequiredExecutors() {
    return config.getNumberOfReaders();
  }

}
