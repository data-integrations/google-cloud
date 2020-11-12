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

import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Wrapper class for Pub/Sub message contents.
 * <p>
 * We use this class to mirror the contents of Google's com.google.pubsub.v1.ReceivedMessage without having to add
 * an additional exported dependency for our workers.
 */
public class PubSubMessage implements Externalizable {

  private String messageId;
  private String orderingKey;
  private String ackId;
  private byte[] data;
  private Map<String, String> attributes;
  private Instant publishTime;

  @VisibleForTesting
  public PubSubMessage() {
  }

  @VisibleForTesting
  public PubSubMessage(@Nullable String messageId, @Nullable String orderingKey, @Nullable String ackId,
                       @Nullable byte[] data, @Nullable Map<String, String> attributes, @Nullable Instant publishTime) {
    this.messageId = messageId;
    this.orderingKey = orderingKey;
    this.ackId = ackId;
    this.data = data;
    this.attributes = attributes;
    this.publishTime = publishTime;
  }

  public PubSubMessage(@Nullable ReceivedMessage message) {
    if (message == null) {
      return;
    }

    PubsubMessage psMessage = message.getMessage();

    if (psMessage != null) {
      this.messageId = psMessage.getMessageId();
      this.orderingKey = psMessage.getOrderingKey();

      if (psMessage.getData() != null) {
        this.data = psMessage.getData().toByteArray();
      }

      if (psMessage.getAttributesMap() != null) {
        this.attributes = new HashMap<>(psMessage.getAttributesMap());
      }

      if (psMessage.getPublishTime() != null) {
        this.publishTime = Instant.ofEpochSecond(psMessage.getPublishTime().getSeconds())
          .plusNanos(psMessage.getPublishTime().getNanos());
      }
    }

    this.ackId = message.getAckId();
  }

  @Nullable
  public String getMessageId() {
    return messageId;
  }

  @Nullable
  public String getOrderingKey() {
    return orderingKey;
  }

  @Nullable
  public String getAckId() {
    return ackId;
  }

  @Nullable
  public byte[] getData() {
    return data;
  }

  @Nullable
  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Nullable
  public Instant getPublishTime() {
    return publishTime;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    writeString(out, messageId);
    writeString(out, orderingKey);
    writeString(out, ackId);
    writeInstant(out, publishTime);

    out.writeInt(attributes != null ? attributes.size() : -1);
    if (attributes != null) {
      for (Map.Entry<String, String> entry : attributes.entrySet()) {
        writeString(out, entry.getKey());
        writeString(out, entry.getValue());
      }
    }

    out.writeInt(data != null ? data.length : -1);
    if (data != null) {
      out.write(data);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.messageId = readString(in);
    this.orderingKey = readString(in);
    this.ackId = readString(in);
    this.publishTime = readInstant(in);

    //Rebuild attributes map
    int mapSize = in.readInt();
    if (mapSize >= 0) {
      this.attributes = new HashMap<>();
    }
    for (int i = 0; i < mapSize; i++) {
      String k = readString(in);
      String v = readString(in);
      this.attributes.put(k, v);
    }

    //Read data
    int dataSize = in.readInt();
    if (dataSize >= 0) {
      this.data = new byte[dataSize];
      in.readFully(this.data, 0, dataSize);
    }
  }

  private void writeString(ObjectOutput out, String str) throws IOException {
    if (str == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeUTF(str);
    }
  }

  private String readString(ObjectInput in) throws IOException {
    boolean isNotNull = in.readBoolean();

    if (isNotNull) {
      return in.readUTF();
    } else {
      return null;
    }
  }

  private void writeInstant(ObjectOutput out, Instant instant) throws IOException {
    if (instant == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeLong(instant.getEpochSecond());
      out.writeInt(instant.getNano());
    }
  }

  private Instant readInstant(ObjectInput in) throws IOException {
    boolean isNotNull = in.readBoolean();

    if (isNotNull) {
      long epoch = in.readLong();
      int nano = in.readInt();
      return Instant.ofEpochSecond(epoch, nano);
    } else {
      return null;
    }
  }
}
