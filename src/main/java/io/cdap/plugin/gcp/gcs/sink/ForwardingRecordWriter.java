/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.gcp.common.ExceptionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

/**
 * ForwardingRecordWriter which delegates all operations to another RecordWriter.
 * <p>
 * This is used to wrap the RecordWriter of the delegate format and
 * throw {@link io.cdap.cdap.api.exception.ProgramFailureException} from IOException.</p>
 */
public class ForwardingRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
  private final RecordWriter delegate;

  /**
   * Constructor for ForwardingRecordWriter.
   * @param delegate the delegate RecordWriter
   */
  public ForwardingRecordWriter(RecordWriter delegate) {
    this.delegate = delegate;
  }

  @Override
  public void write (NullWritable nullWritable, StructuredRecord structuredRecord)
    throws IOException, InterruptedException {
    ExceptionUtils.invokeWithProgramFailureAndInterruptionHandling(
      () -> delegate.write(nullWritable, structuredRecord));
  }

  @Override
  public void close (TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    ExceptionUtils.invokeWithProgramFailureAndInterruptionHandling(
      () -> delegate.close(taskAttemptContext));
  }
}
