/*
 * Copyright © 2021 Cask Data, Inc.
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Output Format used to handle Schemaless Records as input.
 */
public class DelegatingGCSOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {

  public static final String PARTITION_FIELD = "delegating_output_format.partition.field";
  public static final String DELEGATE_CLASS = "delegating_output_format.delegate";
  public static final String OUTPUT_PATH_BASE_DIR = "delegating_output_format.output.path.base";
  public static final String OUTPUT_PATH_SUFFIX = "delegating_output_format.output.path.suffix";

  public DelegatingGCSOutputFormat() {
  }

  /**
   * Get required configuration properties for this Output Format
   */
  public static Map<String, String> configure(String delegateClassName,
                                              String filterField,
                                              String outputBaseDir,
                                              String outputSuffix) {
    Map<String, String> config = new HashMap<>();
    config.put(DELEGATE_CLASS, delegateClassName);
    config.put(PARTITION_FIELD, filterField);
    config.put(OUTPUT_PATH_BASE_DIR, outputBaseDir);
    config.put(OUTPUT_PATH_SUFFIX, outputSuffix);
    return config;
  }

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext context) {
    Configuration hConf = context.getConfiguration();
    String partitionField = hConf.get(PARTITION_FIELD);

    return new DelegatingGCSRecordWriter(context, partitionField, getOutputCommitter(context));
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    //no-op
  }

  @Override
  public DelegatingGCSOutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new DelegatingGCSOutputCommitter(context);
  }

}
