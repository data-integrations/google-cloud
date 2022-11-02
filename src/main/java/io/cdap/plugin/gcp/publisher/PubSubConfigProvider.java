/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.gcp.publisher;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.dynamic.BatchConfigProvider;
import io.cdap.plugin.gcp.publisher.source.PubSubSubscriberConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Reads pipeline configs from pub/sub.
 */
@Plugin(type = BatchConfigProvider.PLUGIN_TYPE)
@Name("PubSub")
public class PubSubConfigProvider implements BatchConfigProvider {
  private final PubSubSubscriberConfig config;

  public PubSubConfigProvider(PubSubSubscriberConfig config) {
    this.config = config;
  }

  @Override
  public String get() {
    GrpcSubscriberStub subscriber = null;
    try {
      subscriber = GrpcSubscriberStub.create(SubscriberStubSettings.newBuilder().build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String subscription = ProjectSubscriptionName.format(config.getProject(), config.getSubscription());
    PullRequest pullRequest =
      PullRequest.newBuilder()
        .setMaxMessages(1)
        .setSubscription(subscription)
        .build();
    PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

    List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();

    if (receivedMessages.isEmpty()) {
      return null;
    }

    ReceivedMessage message = receivedMessages.iterator().next();

    AcknowledgeRequest acknowledgeRequest =
      AcknowledgeRequest.newBuilder()
        .setSubscription(subscription)
        .addAckIds(message.getAckId())
        .build();
    subscriber.acknowledgeCallable().call(acknowledgeRequest);

    return message.getMessage().getData().toString(StandardCharsets.UTF_8);
  }

}
