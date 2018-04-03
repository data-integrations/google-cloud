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

package co.cask.common;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * An input format that reads the whole file content as one record.
 */
public class WholeFileInputFormat extends FileInputFormat<String, BytesWritable> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public WholeFileRecordReader createRecordReader(InputSplit inputSplit,
                                                  TaskAttemptContext context) throws IOException, InterruptedException {
    WholeFileRecordReader reader = new WholeFileRecordReader();
    reader.initialize(inputSplit, context);
    return reader;
  }
}