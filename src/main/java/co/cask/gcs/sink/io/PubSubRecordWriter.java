/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.gcs.sink.io;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * Writer that writes a single record
 */
public class PubSubRecordWriter extends RecordWriter<NullWritable, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubRecordWriter.class);
  private Publisher publisher;

  public PubSubRecordWriter(final String projectId, final String topicId,
                            final String credentialsPath) throws IOException {

    TopicName topic = TopicName.create(projectId, topicId);
    this.publisher = Publisher.defaultBuilder(topic)
      .setCredentialsProvider(new FixedCredentialsProvider() {
        @Nullable
        @Override
        public Credentials getCredentials() {
          return loadCredentials(credentialsPath);
        }
      }).build();
  }

  private Credentials loadCredentials(String credentialsPath) {
    File path = new File(credentialsPath);
    if (!path.exists()) {
      throw new IllegalArgumentException("credentialsPath does not exist");
    }

    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Exception reading from credentials file %s",
                                                       credentialsPath));
    }
  }

  @Override
  public void write(NullWritable key, Text value) throws IOException {
    if (publisher == null) {
      throw new IOException("PubSub publisher is not initialized");
    }
    ByteString data = ByteString.copyFromUtf8(value.toString());
    PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();

    try {
      publisher.publish(message).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    try {
      publisher.shutdown();
    } catch (Exception e) {
      LOG.warn("Error while shutting down publisher", e.getMessage());
    }
  }
}
