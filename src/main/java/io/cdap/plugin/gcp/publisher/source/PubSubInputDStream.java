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

import com.google.auth.Credentials;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.receiver.Receiver;

import javax.annotation.Nullable;

/**
 * Input Stream used to subscribe to a Pub/Sub topic and pull messages.
 */
public class PubSubInputDStream extends ReceiverInputDStream<PubSubMessage> {

  private final PubSubSubscriberConfig config;
  private final StorageLevel storageLevel;
  private final boolean autoAcknowledge;

  /**
   * Constructor Method
   *
   * @param streamingContext Spark Streaming Context
   * @param config           Configuration
   * @param storageLevel     Spark Storage Level for received messages
   * @param autoAcknowledge  Acknowledge messages
   */
  PubSubInputDStream(StreamingContext streamingContext, PubSubSubscriberConfig config, StorageLevel storageLevel,
                     boolean autoAcknowledge) {
    super(streamingContext, scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class));
    this.config = config;
    this.storageLevel = storageLevel;
    this.autoAcknowledge = autoAcknowledge;
  }


  @Override
  public Receiver<PubSubMessage> getReceiver() {
    return new PubSubReceiver(this.config,
                              this.autoAcknowledge,
                              this.storageLevel);
  }
}
