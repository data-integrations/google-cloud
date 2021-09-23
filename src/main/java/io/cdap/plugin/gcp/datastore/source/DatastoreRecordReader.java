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

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.protobuf.ByteString;
import io.cdap.plugin.gcp.datastore.source.util.DatastoreSourceConstants;
import io.cdap.plugin.gcp.datastore.util.DatastoreUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Datastore read reader instantiates a record reader that will read the entities from Datastore,
 * using given {@link Query} instance from input split.
 */
public class DatastoreRecordReader extends RecordReader<LongWritable, Entity> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreRecordReader.class);

  private Datastore datastore;
  private Iterator<EntityResult> results;
  private Entity entity;
  private List<EntityResult> entityResultList;
  private int batchSerializedSize;
  private long index;
  private LongWritable key;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration config = taskAttemptContext.getConfiguration();
    Query query = ((QueryInputSplit) inputSplit).getQuery();
    datastore = DatastoreUtil.getDatastoreV1(
      config.get(DatastoreSourceConstants.CONFIG_SERVICE_ACCOUNT),
      config.getBoolean(DatastoreSourceConstants.CONFIG_SERVICE_ACCOUNT_IS_FILE, true),
      config.get(DatastoreSourceConstants.CONFIG_PROJECT));
    LOG.trace("Executing query split: {}", query);
    RunQueryRequest request = RunQueryRequest.newBuilder()
      .setQuery(query)
      // partition id needs to be set in the RunQueryRequest in addition to being passed to QuerySplitter.getSplits.
      // This is a quirk of the V1 API.
      .setPartitionId(PartitionId.newBuilder()
                        .setNamespaceId(config.get(DatastoreSourceConstants.CONFIG_NAMESPACE))
                        .setProjectId(config.get(DatastoreSourceConstants.CONFIG_PROJECT)))
      .build();

    entityResultList = new ArrayList<>();
    createEntityResultsList(query, request);
    taskAttemptContext.getCounter(FileInputFormatCounter.BYTES_READ).increment(batchSerializedSize);
    results = entityResultList.iterator();
    index = 0;
  }

  private void createEntityResultsList(Query query, RunQueryRequest request) throws IOException {
    try {
      QueryResultBatch batch = datastore.runQuery(request).getBatch();
      batchSerializedSize += batch.getSerializedSize();
      entityResultList.addAll(batch.getEntityResultsList());
      LOG.debug("Batch moreResultsType: {}", batch.getMoreResults());
      // Datastore API only returns up to 300 items. Need to use cursor pagination if there is more results
      if (batch.getMoreResults() != QueryResultBatch.MoreResultsType.NO_MORE_RESULTS) {
        ByteString cursor = batch.getEndCursor();
        query = query.toBuilder().setStartCursor(cursor).build();
        request = request.toBuilder().setQuery(query).build();
        createEntityResultsList(query, request);
      }
    } catch (DatastoreException e) {
      throw new IOException("Failed to run query", e);
    }
  }

  @Override
  public boolean nextKeyValue() {
    if (!results.hasNext()) {
      return false;
    }
    entity = results.next().getEntity();
    key = new LongWritable(index);
    ++index;
    return true;
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Entity getCurrentValue() {
    return entity;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() {
  }

}
