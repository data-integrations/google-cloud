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

import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ResultSet;
import io.cdap.plugin.gcp.spanner.SpannerConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Spanner input format, deserializes and gets the partitions list from configuration to create input splits.
 */
public class SpannerInputFormat extends InputFormat<NullWritable, ResultSet> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerInputFormat.class);

  /**
   * Deserialize partitions list from configuration and create {@link PartitionInputSplit PartitionInputSplits}
   * using them.
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    Configuration configuration = jobContext.getConfiguration();
    List<InputSplit> partitionSplits = new ArrayList<>();
    try {
      ArrayList<Partition> partitions = deserializeObject(configuration, SpannerConstants.PARTITIONS_LIST);
      for (Partition partition : partitions) {
        PartitionInputSplit split = new PartitionInputSplit(partition);
        partitionSplits.add(split);
      }
    } catch (Exception e) {
      throw new IOException("Exception while trying to initialize spanner and create partition splits", e);
    }
    LOG.debug("Initialized and configured {} splits", partitionSplits.size());
    return partitionSplits;
  }

  private <T> T deserializeObject(Configuration configuration, String property) throws IOException {
    String propertyValue = configuration.get(property);
    if (propertyValue == null) {
      throw new IOException(String.format("Spanner %s is not available in the configuration", property));
    }
    byte[] propertyValueBytes = Base64.getDecoder().decode(propertyValue);
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(propertyValueBytes);
         ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
      return (T) objectInputStream.readObject();
    } catch (ClassNotFoundException cfe) {
      throw new IOException("Exception while trying to deserialize object ", cfe);
    }
  }

  @Override
  public RecordReader<NullWritable, ResultSet> createRecordReader(
    InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    BatchTransactionId batchTransactionId =
      deserializeObject(configuration, SpannerConstants.SPANNER_BATCH_TRANSACTION_ID);
    return new SpannerRecordReader(batchTransactionId);
  }
}
