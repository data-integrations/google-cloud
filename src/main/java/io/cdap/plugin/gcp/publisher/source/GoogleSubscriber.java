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
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Realtime source plugin to read from Google PubSub.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("GoogleSubscriber")
@Description("Streaming Source to read messages from Google PubSub.")
public class GoogleSubscriber extends PubSubSubscriber<StructuredRecord> {
  private static final Schema DEFAULT_SCHEMA =
    Schema.recordOf("event",
                    Schema.Field.of("message", Schema.of(Schema.Type.BYTES)),
                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                    Schema.Field.of("attributes", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                               Schema.of(Schema.Type.STRING)))
    );

  private static final SerializableFunction<PubSubMessage, StructuredRecord> MAPPING_FUNCTION =
    new SerializableFunction<PubSubMessage, StructuredRecord>() {
      @Override
      public StructuredRecord apply(PubSubMessage receivedMessage) {
        return StructuredRecord.builder(DEFAULT_SCHEMA)
          .set("message", receivedMessage.getData())
          .set("id", receivedMessage.getMessageId())
          .setTimestamp("timestamp", getTimestamp(receivedMessage.getPublishTime()))
          .set("attributes", receivedMessage.getAttributes())
          .build();
      }
    };

  private static ZonedDateTime getTimestamp(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

  public GoogleSubscriber(PubSubSubscriberConfig config) {
    super(config, DEFAULT_SCHEMA, MAPPING_FUNCTION);
  }
}
