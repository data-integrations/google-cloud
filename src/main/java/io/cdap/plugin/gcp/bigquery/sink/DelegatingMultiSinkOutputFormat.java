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

package io.cdap.plugin.gcp.bigquery.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.UUID;

/**
 * Output Format used to handle the use case where the output schema is not set as a pipeline argument.
 */
public class DelegatingMultiSinkOutputFormat extends OutputFormat<StructuredRecord, NullWritable> {
  private static final String TABLENAME_FIELD = "bq.delegating.multi.tablename.field";
  private static final String BUCKET_NAME = "bq.delegating.multi.bucket";
  private static final String BUCKET_PATH_UNIQUE_ID = "bq.delegating.multi.bucket.path.uuid";
  private static final String PROJECT_NAME = "bq.delegating.multi.project";
  private static final String DATASET_NAME = "bq.delegating.multi.dataset";

  private DelegatingMultiSinkOutputCommitter delegatingMultiSinkOutputCommitter = null;

  public static void configure(Configuration conf,
                               String filterField,
                               String bucketName,
                               String projectName,
                               String datasetName) {
    conf.set(TABLENAME_FIELD, filterField);
    conf.set(BUCKET_NAME, bucketName);
    conf.set(BUCKET_PATH_UNIQUE_ID, UUID.randomUUID().toString());
    conf.set(PROJECT_NAME, projectName);
    conf.set(DATASET_NAME, datasetName);
  }

  @Override
  public RecordWriter<StructuredRecord, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = taskAttemptContext.getConfiguration();
    String tableNameField = conf.get(TABLENAME_FIELD);
    String bucketName = conf.get(BUCKET_NAME);
    String bucketPathUniqueId = conf.get(BUCKET_PATH_UNIQUE_ID);
    String projectName = conf.get(PROJECT_NAME);
    String datasetName = conf.get(DATASET_NAME);

    return new DelegatingMultiSinkRecordWriter(taskAttemptContext,
                                               tableNameField,
                                               bucketName,
                                               bucketPathUniqueId,
                                               projectName,
                                               datasetName,
                                               getOutputCommitterInstance(taskAttemptContext));
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
    //no-op
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    return getOutputCommitterInstance(taskAttemptContext);
  }

  private DelegatingMultiSinkOutputCommitter getOutputCommitterInstance(TaskAttemptContext taskAttemptContext) {
    if (delegatingMultiSinkOutputCommitter == null) {
      Configuration conf = taskAttemptContext.getConfiguration();
      String projectName = conf.get(PROJECT_NAME);
      String datasetName = conf.get(DATASET_NAME);
      String bucketName = conf.get(BUCKET_NAME);
      String bucketPathUniqueId = conf.get(BUCKET_PATH_UNIQUE_ID);
      delegatingMultiSinkOutputCommitter = new DelegatingMultiSinkOutputCommitter(projectName,
                                                                                  datasetName,
                                                                                  bucketName,
                                                                                  bucketPathUniqueId);
    }

    return delegatingMultiSinkOutputCommitter;
  }
}
