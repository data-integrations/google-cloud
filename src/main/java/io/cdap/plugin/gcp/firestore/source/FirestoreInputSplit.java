/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.gcp.firestore.source;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Firestore input split.
 */
public class FirestoreInputSplit extends InputSplit implements Writable {
  private long start = 0L;
  private long end = 0L;

  public FirestoreInputSplit() {
  }

  public FirestoreInputSplit(long start, long end) {
    this.start = start;
    this.end = end;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(this.start);
    dataOutput.writeLong(this.end);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.start = dataInput.readLong();
    this.end = dataInput.readLong();
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return this.end - this.start;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }
}
