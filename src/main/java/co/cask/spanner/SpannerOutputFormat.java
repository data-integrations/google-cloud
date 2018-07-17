/*
 * Copyright © 2018 Google Inc.
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

package co.cask.spanner;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.common.NoOpOutputCommitter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class SpannerOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {
  @Override
  public RecordWriter<NullWritable, StructuredRecord>
  getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new SpannerRecordWriter(context.getConfiguration());
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
    // no-op
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    return new NoOpOutputCommitter();
  }
}
