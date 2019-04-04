/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.plugin.gcp.datastore.source;

import com.google.cloud.datastore.Entity;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.datastore.v1.client.QuerySplitter;
import com.google.protobuf.TextFormat;
import io.cdap.plugin.gcp.datastore.exception.DatastoreExecutionException;
import io.cdap.plugin.gcp.datastore.source.util.DatastoreSourceConstants;
import io.cdap.plugin.gcp.datastore.util.DatastoreUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Datastore input format, splits query from the configuration into list of queries
 * using {@link QuerySplitter} in order to create input splits.
 */
public class DatastoreInputFormat extends InputFormat<LongWritable, Entity> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreInputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    Configuration config = jobContext.getConfiguration();
    String queryString = config.get(DatastoreSourceConstants.CONFIG_QUERY);
    Query.Builder queryBuilder = Query.newBuilder();
    TextFormat.merge(queryString, queryBuilder);

    Query query = queryBuilder.build();
    LOG.debug("Query to be split: {}", query);
    PartitionId partitionId = PartitionId.newBuilder()
      .setNamespaceId(config.get(DatastoreSourceConstants.CONFIG_NAMESPACE))
      .setProjectId(config.get(DatastoreSourceConstants.CONFIG_PROJECT))
      .build();
    int numSplits = config.getInt(DatastoreSourceConstants.CONFIG_NUM_SPLITS, 1);
    Datastore datastore = DatastoreUtil.getDatastoreV1(
      config.get(DatastoreSourceConstants.CONFIG_SERVICE_ACCOUNT_FILE_PATH),
      config.get(DatastoreSourceConstants.CONFIG_PROJECT));
    QuerySplitter querySplitter = DatastoreHelper.getQuerySplitter();

    try {
      List<Query> splits = querySplitter.getSplits(query, partitionId, numSplits, datastore);
      LOG.debug("Split query into {} splits, requested number of splits: {}", splits.size(), numSplits);
      return splits.stream()
        .map(QueryInputSplit::new)
        .collect(Collectors.toList());
    } catch (DatastoreException e) {
      throw new DatastoreExecutionException("Unable to split the query: " + query, e);
    }
  }

  @Override
  public RecordReader<LongWritable, Entity> createRecordReader(InputSplit inputSplit,
                                                               TaskAttemptContext taskAttemptContext) {
    return new DatastoreRecordReader();
  }
}
