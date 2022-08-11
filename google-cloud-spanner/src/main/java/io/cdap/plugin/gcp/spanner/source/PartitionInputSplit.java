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

import com.google.cloud.spanner.Partition;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Spanner partition input split. Performs serialization and
 * deserialization of the {@link Partition} received from {@link SpannerInputFormat}.
 */
public class PartitionInputSplit extends InputSplit implements Writable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionInputSplit.class);
  private Partition partition;

  /**
   * This constructor is needed for hadoop deserialization
   */
  public PartitionInputSplit() {

  }

  public PartitionInputSplit(Partition partition) throws Exception {
    this.partition = partition;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(partition);
      objectOutputStream.flush();
      byte[] objectBytes = byteArrayOutputStream.toByteArray();
      // we write the byte array length, to help initialize byte array during deserialization to read from DataInput
      dataOutput.writeInt(objectBytes.length);
      dataOutput.write(objectBytes);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int byteLength = dataInput.readInt();
    byte[] readArray = new byte[byteLength];
    dataInput.readFully(readArray);
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(readArray);
         ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
      partition = (Partition) objectInputStream.readObject();
    } catch (ClassNotFoundException cfe) {
      throw new IOException("Exception while trying to deserialize object ", cfe);
    }
  }

  public Partition getPartition() {
    return partition;
  }
}
