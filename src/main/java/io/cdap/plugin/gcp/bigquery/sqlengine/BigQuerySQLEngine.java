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

package io.cdap.plugin.gcp.bigquery.sqlengine;

import com.google.cloud.bigquery.Job;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchContext;
import io.cdap.cdap.etl.api.engine.sql.BatchSQLEngine;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPullDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPushDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.Map;

public class BigQuerySQLEngine
  extends BatchSQLEngine<LongWritable, GenericData.Record, StructuredRecord, NullWritable> {

  Map<String, Job> bqJobs;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void prepareRun(BatchContext context) throws Exception {
    super.prepareRun(context);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchContext context) {
    super.onRunFinish(succeeded, context);
  }

  @Override
  public SQLPushDataset<StructuredRecord, StructuredRecord, NullWritable> getPushProvider(SQLPushRequest sqlPushRequest) throws SQLEngineException {
    return null;
  }

  @Override
  public SQLPullDataset<StructuredRecord, LongWritable, GenericData.Record> getPullProvider(SQLPullRequest sqlPullRequest) throws SQLEngineException {
    return null;
  }

  @Override
  public boolean exists(String s) throws SQLEngineException {
    return false;
  }

  @Override
  public boolean canJoin(SQLJoinRequest sqlJoinRequest) {
    return true;
  }

  @Override
  public SQLDataset join(SQLJoinRequest sqlJoinRequest) throws SQLEngineException {
    return null;
  }

  @Override
  public void cleanup(String s) throws SQLEngineException {

  }
}
