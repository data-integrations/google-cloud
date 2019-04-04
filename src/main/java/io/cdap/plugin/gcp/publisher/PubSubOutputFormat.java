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

package io.cdap.plugin.gcp.publisher;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * OutputFormat to write to Pub/Sub topic.
 */
public class PubSubOutputFormat extends OutputFormat<NullWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubOutputFormat.class);

  private static final String SERVICE_PATH = "service.path";
  private static final String PROJECT = "project";
  private static final String TOPIC = "topic";
  private static final String COUNT_BATCH_SIZE = "message.count.batch.size";
  private static final String REQUEST_BYTES_THRESHOLD = "request.bytes.threshold";
  private static final String DELAY_THRESHOLD = "delay.threshold";
  private static final String ERROR_THRESHOLD = "error.threshold";
  private static final String RETRY_TIMEOUT_SECONDS = "retry.timeout";

  public static void configure(Configuration configuration, GooglePublisher.Config config) {
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    if (serviceAccountFilePath != null) {
      configuration.set(SERVICE_PATH, config.getServiceAccountFilePath());
    }
    String projectId = config.getProject();
    configuration.set(PROJECT, projectId);
    configuration.set(TOPIC, config.getTopic());
    configuration.set(COUNT_BATCH_SIZE, String.valueOf(config.getMessageCountBatchSize()));
    configuration.set(REQUEST_BYTES_THRESHOLD, String.valueOf(config.getRequestBytesThreshold()));
    configuration.set(DELAY_THRESHOLD, String.valueOf(config.getPublishDelayThresholdMillis()));
    configuration.set(ERROR_THRESHOLD, String.valueOf(config.getErrorThreshold()));
    configuration.set(RETRY_TIMEOUT_SECONDS, String.valueOf(config.getRetryTimeoutSeconds()));
  }

  @Override
  public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration config = taskAttemptContext.getConfiguration();
    String serviceAccountFilePath = config.get(SERVICE_PATH);
    String projectId = config.get(PROJECT);
    String topic = config.get(TOPIC);
    long countSize = Long.parseLong(config.get(COUNT_BATCH_SIZE));
    long bytesThreshold = Long.parseLong(config.get(REQUEST_BYTES_THRESHOLD));
    long delayThreshold = Long.parseLong(config.get(DELAY_THRESHOLD));
    long errorThreshold = Long.parseLong(config.get(ERROR_THRESHOLD));
    int retryTimeout = Integer.parseInt(config.get(RETRY_TIMEOUT_SECONDS));
    Publisher.Builder publisher = Publisher.newBuilder(ProjectTopicName.of(projectId, topic))
      .setBatchingSettings(getBatchingSettings(countSize, bytesThreshold, delayThreshold))
      .setRetrySettings(getRetrySettings(retryTimeout));

    if (serviceAccountFilePath != null) {
      publisher.setCredentialsProvider(() -> GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath));
    }

    return new PubSubRecordWriter(publisher.build(), errorThreshold);
  }

  private RetrySettings getRetrySettings(int maxRetryTimeout) {
    // only maxRetryTimeout is configurable by user, setting defaults for other parameters
    return RetrySettings.newBuilder()
      .setInitialRetryDelay(Duration.ofMillis(100))
      .setRetryDelayMultiplier(2)
      .setMaxRetryDelay(Duration.ofSeconds(2))
      .setTotalTimeout(Duration.ofSeconds(maxRetryTimeout))
      .setInitialRpcTimeout(Duration.ofSeconds(1))
      .setMaxRpcTimeout(Duration.ofSeconds(5))
      .build();
  }

  private BatchingSettings getBatchingSettings(long countSize, long bytesThreshold, long delayThreshold) {
    return BatchingSettings.newBuilder()
      .setElementCountThreshold(countSize)
      .setRequestByteThreshold(bytesThreshold)
      .setDelayThreshold(Duration.ofMillis(delayThreshold))
      .build();
  }

  /**
   * Writer publishes messages to PubSub using the passed Publisher,
   * batching and retrying are performed by Publisher based on how it is configured, If publishing a message fails
   * we maintain error count and we throw exception when this error count exceeds a threshold.
   */
  public class PubSubRecordWriter extends RecordWriter<NullWritable, Text> {
    private final Publisher publisher;
    private final AtomicLong failures;
    private final AtomicReference<Throwable> error;
    private final long errorThreshold;
    private final Set<ApiFuture> futures;

    public PubSubRecordWriter(Publisher publisher, long errorThreshold) throws IOException {
      this.publisher = publisher;
      this.error = new AtomicReference<>();
      this.errorThreshold = errorThreshold;
      this.failures = new AtomicLong(0);
      this.futures = ConcurrentHashMap.newKeySet();
    }

    @Override
    public void write(NullWritable key, Text value) throws IOException {
      handleErrorIfAny();
      ByteString data = ByteString.copyFromUtf8(value.toString());
      PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();
      ApiFuture future = publisher.publish(message);
      futures.add(future);
      ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
        @Override
        public void onFailure(Throwable throwable) {
          error.set(throwable);
          failures.incrementAndGet();
          futures.remove(future);
        }

        @Override
        public void onSuccess(String s) {
          futures.remove(future);
        }
      });
    }

    private void handleErrorIfAny() throws IOException {
      if (failures.get() > errorThreshold) {
        throw new IOException(String.format("Failed to publish %s records", failures.get()), error.get());
      }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
      try {
        // wait till the pending futures are complete before calling shutdown
        publisher.publishAllOutstanding();
        for (ApiFuture future : futures) {
          future.get();
          handleErrorIfAny();
        }
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException("Error publishing records to PubSub", e);
      } finally {
        try {
          publisher.shutdown();
        } catch (Exception e) {
          // if there is an exception while shutting down, we only log
          LOG.debug("Exception while shutting down publisher ", e);
        }
      }
    }
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    return new OutputCommitter() {

      @Override
      public void setupJob(JobContext jobContext) {

      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) {

      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) {

      }
    };
  }
}
