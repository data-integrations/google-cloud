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

package io.cdap.plugin.gcp.bigtable.sink;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;

import java.io.IOException;

/**
 * Table output format class - extends default {@link TableOutputFormat} in order to override checkOutputSpecs method
 * to include configuration properties before calling `ConnectionFactory.createConnection`.
 * Fixes null pointer exception during connection creation.
 * @param <KEY> the key is ignored
 */
public class BigtableOutputFormat<KEY> extends TableOutputFormat<KEY> {
  public BigtableOutputFormat() {

  }

  @Override
  public RecordWriter<KEY, Mutation> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    RecordWriter<KEY, Mutation> recordWriter = super.getRecordWriter(context);
    return new RecordWriter<KEY, Mutation>() {
      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        recordWriter.close(context);
      }

      @Override
      public void write(KEY key, Mutation value) throws IOException, InterruptedException {
        context.getCounter(FileOutputFormatCounter.BYTES_WRITTEN).increment(value.getRow().length);
        recordWriter.write(key, value);
      }
    };
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    // setting configuration properties (including credentials) before `ConnectionFactory.createConnection` is called
    // in order to prevent null pointer exception during connection creation.
    setConf(context.getConfiguration());
    super.checkOutputSpecs(context);
  }
}
