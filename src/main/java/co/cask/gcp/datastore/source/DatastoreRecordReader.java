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
package co.cask.gcp.datastore.source;

import co.cask.gcp.datastore.source.util.DatastoreSourceQueryUtil;
import co.cask.gcp.datastore.util.DatastoreUtil;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.CONFIG_PROJECT;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.CONFIG_SERVICE_ACCOUNT_FILE_PATH;

/**
 * Datastore read reader instantiates a record reader that will read the entities from Datastore,
 * using given {@link Query} instance from input split.
 */
public class DatastoreRecordReader extends RecordReader<LongWritable, Entity> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreRecordReader.class);

  private QueryResults<Entity> results;
  private Entity entity;
  private long index;
  private LongWritable key;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    Configuration config = taskAttemptContext.getConfiguration();
    Query<Entity> query = DatastoreSourceQueryUtil.transformPbQuery(((QueryInputSplit) inputSplit).getQuery(), config);
    Datastore datastore = DatastoreUtil.getDatastore(config.get(CONFIG_SERVICE_ACCOUNT_FILE_PATH),
                                                     config.get(CONFIG_PROJECT));
    LOG.trace("Executing query split: {}", query);
    results = datastore.run(query);
    index = 0;
  }

  @Override
  public boolean nextKeyValue() {
    if (!results.hasNext()) {
      return false;
    }
    entity = results.next();
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
