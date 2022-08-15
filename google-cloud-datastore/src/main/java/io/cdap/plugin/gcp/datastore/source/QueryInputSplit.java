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

import com.google.datastore.v1.Query;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Datastore query input split. Performs serialization and
 * deserialization of the {@link Query} received from {@link DatastoreInputFormat}.
 */
public class QueryInputSplit extends InputSplit implements Writable {

  private Query query;

  public QueryInputSplit() {
    // is needed for Hadoop deserialization
  }

  public QueryInputSplit(Query query) {
    this.query = query;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    byte[] bytes = query.toByteArray();
    dataOutput.writeInt(bytes.length);
    dataOutput.write(bytes);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int length = dataInput.readInt();
    byte[] bytes = new byte[length];
    dataInput.readFully(bytes);
    query = Query.parseFrom(bytes);
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }

  public Query getQuery() {
    return query;
  }

}
