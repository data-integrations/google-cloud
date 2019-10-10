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

package io.cdap.plugin.gcp.spanner.source;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import io.cdap.plugin.gcp.spanner.SpannerConstants;
import io.cdap.plugin.gcp.spanner.common.SpannerUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Spanner record reader - updates result set during iteration
 */
public class SpannerRecordReader extends RecordReader<NullWritable, ResultSet> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerRecordReader.class);
  private final BatchTransactionId batchTransactionId;
  private ResultSet resultSet;

  public SpannerRecordReader(BatchTransactionId batchTransactionId) {
    this.batchTransactionId = batchTransactionId;
  }

  @Override
  public void initialize(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    PartitionInputSplit partitionInputSplit = (PartitionInputSplit) inputSplit;
    try {
      Configuration configuration = taskAttemptContext.getConfiguration();
      Spanner spanner = SpannerUtil.getSpannerService(configuration.get(SpannerConstants.SERVICE_ACCOUNT_FILE_PATH),
                                                      configuration.get(SpannerConstants.PROJECT_ID));
      BatchClient batchClient = spanner.getBatchClient(
        DatabaseId.of(configuration.get(SpannerConstants.PROJECT_ID),
                      configuration.get(SpannerConstants.INSTANCE_ID), configuration.get(SpannerConstants.DATABASE)));
      BatchReadOnlyTransaction transaction = batchClient.batchReadOnlyTransaction(batchTransactionId);
      resultSet = transaction.execute(partitionInputSplit.getPartition());
    } catch (Exception e) {
      throw new IOException("Exception while trying to execute query to get result set ", e);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return resultSet != null && resultSet.next();
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public ResultSet getCurrentValue() throws IOException, InterruptedException {
    return resultSet;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0.0f;
  }

  @Override
  public void close() throws IOException {
    LOG.trace("Closing Record reader");
    resultSet.close();
  }
}
