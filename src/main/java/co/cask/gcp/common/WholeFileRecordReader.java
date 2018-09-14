/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.gcp.common;

import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * A {@link RecordReader} that reads the full file content.
 */
final class WholeFileRecordReader extends RecordReader<String, BytesWritable> {

  private String filePath;
  private BytesWritable value;
  private FileSplit inputSplit;
  private Configuration hConf;

  @Override
  public void initialize(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (!(inputSplit instanceof FileSplit)) {
      // This shouldn't happen
      throw new IllegalArgumentException("Input split should be instance of FileSplit: " + inputSplit.getClass());
    }
    if (inputSplit.getLength() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Content cannot be larger than " + Integer.MAX_VALUE / 1024 / 1024 + "MB");
    }
    this.inputSplit = (FileSplit) inputSplit;
    this.hConf = taskAttemptContext.getConfiguration();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (value != null) {
      return false;
    }

    Path path = inputSplit.getPath();
    FileSystem fs = path.getFileSystem(hConf);
    filePath = path.toUri().getPath();
    try (FSDataInputStream input = fs.open(path)) {
      byte[] content = new byte[(int) inputSplit.getLength()];
      ByteStreams.readFully(input, content);
      value = new BytesWritable(content);
    }
    return true;
  }

  @Override
  public String getCurrentKey() throws IOException, InterruptedException {
    return filePath;
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    return value == null ? new BytesWritable() : value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return value == null ? 0.0f : 1.0f;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}