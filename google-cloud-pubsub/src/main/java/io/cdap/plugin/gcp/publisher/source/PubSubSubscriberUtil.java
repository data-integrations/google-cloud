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

import io.cdap.cdap.etl.api.streaming.StreamingContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;

import java.util.ArrayList;

/**
 * Utility class to create a JavaDStream of received messages.
 */
public final class PubSubSubscriberUtil {

  protected static final Logger LOG = LoggerFactory.getLogger(PubSubSubscriberUtil.class);

  private PubSubSubscriberUtil() {
  }

  /**
   * Get a JavaDStream of received PubSubMessages.
   *
   * @param streamingContext the screaming context
   * @param config           The subscriver configuration
   * @return JavaDStream of all received pub/sub messages.
   * @throws Exception when the credentials could not be loaded.
   */
  public static JavaDStream<PubSubMessage> getStream(StreamingContext streamingContext,
                                                     PubSubSubscriberConfig config) throws Exception {
    boolean autoAcknowledge = true;
    if (streamingContext.isPreviewEnabled()) {
      autoAcknowledge = false;
    }

    JavaDStream<PubSubMessage> stream =
      getInputDStream(streamingContext, config, autoAcknowledge);

    return stream;
  }

  /**
   * Get a merged JavaDStream containing all received messages from multiple receivers.
   *
   * @param streamingContext the streaming context
   * @param config           subscriber config
   * @param autoAcknowledge  if the messages should be acknowleged or not.
   * @return JavaDStream containing all received messages.
   */
  @SuppressWarnings("unchecked")
  protected static JavaDStream<PubSubMessage> getInputDStream(StreamingContext streamingContext,
                                                              PubSubSubscriberConfig config,
                                                              boolean autoAcknowledge) {
    ArrayList<DStream<PubSubMessage>> receivers = new ArrayList<>(config.getNumberOfReaders());
    ClassTag<PubSubMessage> tag = scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class);

    for (int i = 1; i <= config.getNumberOfReaders(); i++) {
      ReceiverInputDStream<PubSubMessage> receiverInputDStream =
        new PubSubInputDStream(streamingContext.getSparkStreamingContext().ssc(), config, StorageLevel.MEMORY_ONLY(),
                               autoAcknowledge);
      receivers.add(receiverInputDStream);
    }

    DStream<PubSubMessage> dStream = streamingContext.getSparkStreamingContext().ssc()
      .union(JavaConverters.collectionAsScalaIterableConverter(receivers).asScala().toSeq(), tag);

    return new JavaDStream<>(dStream, tag);
  }

}
