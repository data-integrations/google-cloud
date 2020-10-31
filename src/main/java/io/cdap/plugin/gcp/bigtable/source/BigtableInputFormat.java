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

package io.cdap.plugin.gcp.bigtable.source;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;

import java.io.IOException;

/**
 * Bigtable input format, keeps track of bytes read.
 */

public class BigtableInputFormat extends TableInputFormat {

  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(InputSplit split,
                                                                         TaskAttemptContext context)
    throws IOException {
    RecordReader<ImmutableBytesWritable, Result> recordReader = super.createRecordReader(split, context);
    return new RecordReader<ImmutableBytesWritable, Result>() {
      private Counter readBytes;
      @Override
      public void close() throws IOException {
        recordReader.close();
      }

      @Override
      public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
        return recordReader.getCurrentKey();
      }

      @Override
      public Result getCurrentValue() throws IOException, InterruptedException {
        Result value = recordReader.getCurrentValue();
        readBytes.increment(value.getRow().length);
        return value;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return recordReader.getProgress();
      }

      @Override
      public void initialize(InputSplit inputsplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        readBytes = context.getCounter(FileInputFormatCounter.BYTES_READ);
        recordReader.initialize(inputsplit, context);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return recordReader.nextKeyValue();
      }
    };

  }
}
