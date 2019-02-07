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
package co.cask.gcp.datastore.sink;

import co.cask.gcp.datastore.sink.util.DatastoreSinkConstants;
import co.cask.gcp.datastore.util.DatastoreUtil;
import com.google.cloud.datastore.Batch;
import com.google.cloud.datastore.Batch.Response;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.FullEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DatastoreRecordWriter} writes the job outputs to the Datastore. Accepts <code>null</code> key, FullEntity
 * pairs but writes only FullEntities to the Datastore.
 */
public class DatastoreRecordWriter extends RecordWriter<NullWritable, FullEntity<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreRecordWriter.class);

  private final Datastore datastore;
  private final int batchSize;
  private final boolean useAutogeneratedKey;
  private Batch batch;
  private int totalCount;
  private int numberOfRecordsInBatch;

  public DatastoreRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration config = taskAttemptContext.getConfiguration();
    String projectId = config.get(DatastoreSinkConstants.CONFIG_PROJECT);
    String serviceAccountFilePath = config.get(DatastoreSinkConstants.CONFIG_SERVICE_ACCOUNT_FILE_PATH);
    this.batchSize = config.getInt(DatastoreSinkConstants.CONFIG_BATCH_SIZE, 25);
    this.useAutogeneratedKey = config.getBoolean(DatastoreSinkConstants.CONFIG_USE_AUTOGENERATED_KEY, false);
    LOG.debug("Initialize RecordWriter(projectId={}, batchSize={}, useAutogeneratedKey={}, "
      + "serviceFilePath={})", projectId, batchSize, useAutogeneratedKey, serviceAccountFilePath);

    this.datastore = DatastoreUtil.getDatastore(serviceAccountFilePath, projectId);
    this.batch = datastore.newBatch();
    this.totalCount = 0;
    this.numberOfRecordsInBatch = 0;
  }

  @Override
  public void write(NullWritable key, FullEntity<?> entity) {
    LOG.trace("RecordWriter write({})", entity);
    if (useAutogeneratedKey) {
      batch.putWithDeferredIdAllocation(entity);
    } else {
      batch.put(entity);
    }
    ++totalCount;
    ++numberOfRecordsInBatch;
    if (totalCount % batchSize == 0) {
      flush();
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    flush();
    LOG.debug("Total number of values written to Cloud Datastore: {}", totalCount);
  }

  private void flush() {
    if (numberOfRecordsInBatch > 0) {
      LOG.debug("Writing a batch of {} values to Cloud Datastore.", numberOfRecordsInBatch);
      Response submit = batch.submit();
      LOG.trace("Generated keys: {}", submit.getGeneratedKeys());
      batch = datastore.newBatch();
      numberOfRecordsInBatch = 0;
    }
  }
}
