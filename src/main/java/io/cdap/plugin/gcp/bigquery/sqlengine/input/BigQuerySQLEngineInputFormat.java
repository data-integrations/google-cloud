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

package io.cdap.plugin.gcp.bigquery.sqlengine.input;

import io.cdap.plugin.gcp.bigquery.source.PartitionedBigQueryInputFormat;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

/**
 * BigQuery input format which extends from {@link PartitionedBigQueryInputFormat} and uses
 * {@link BigQuerySQLEngineAvroInputFormat} as the delegate input format.
 */
public class BigQuerySQLEngineInputFormat extends PartitionedBigQueryInputFormat {

  protected InputFormat<LongWritable, GenericData.Record> delegateInputFormat =
    new BigQuerySQLEngineAvroInputFormat();

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    processQuery(context);

    return delegateInputFormat.getSplits(context);
  }
}
