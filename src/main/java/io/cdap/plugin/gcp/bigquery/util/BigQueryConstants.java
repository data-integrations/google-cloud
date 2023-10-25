/*
 * Copyright © 2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */
package io.cdap.plugin.gcp.bigquery.util;

/**
 * BigQuery constants.
 */
public interface BigQueryConstants {

  String CONFIG_ALLOW_SCHEMA_RELAXATION = "cdap.bq.sink.allow.schema.relaxation";
  String CONFIG_ALLOW_SCHEMA_RELAXATION_ON_EMPTY_OUTPUT = "cdap.bq.sink.allow.schema.relaxationoemptyoutput";
  String CONFIG_DESTINATION_TABLE_EXISTS = "cdap.bq.sink.destination.table.exists";
  String CONFIG_PARTITION_BY_FIELD = "cdap.bq.sink.partition.by.field";
  String CONFIG_REQUIRE_PARTITION_FILTER = "cdap.bq.sink.require.partition.filter";
  String CONFIG_PARTITION_FROM_DATE = "cdap.bq.source.partition.from.date";
  String CONFIG_PARTITION_TO_DATE = "cdap.bq.source.partition.to.date";
  String CONFIG_SERVICE_ACCOUNT = "cdap.bq.service.account";
  String CONFIG_SERVICE_ACCOUNT_IS_FILE = "cdap.bq.service.account.isfile";
  String CONFIG_CLUSTERING_ORDER = "cdap.bq.sink.clustering.order";
  String CONFIG_OPERATION = "cdap.bq.sink.operation";
  String CONFIG_TABLE_KEY = "cdap.bq.sink.table.key";
  String CONFIG_DEDUPE_BY = "cdap.bq.sink.dedupe.by";
  String CONFIG_TABLE_FIELDS = "cdap.bq.sink.table.fields";
  String CONFIG_JSON_STRING_FIELDS = "cdap.bq.sink.json.string.fields";
  String CONFIG_FILTER = "cdap.bq.source.filter";
  String CONFIG_PARTITION_FILTER = "cdap.bq.sink.partition.filter";
  String CONFIG_JOB_ID = "cdap.bq.sink.job.id";
  String CONFIG_VIEW_MATERIALIZATION_PROJECT = "cdap.bq.source.view.materialization.project";
  String CONFIG_VIEW_MATERIALIZATION_DATASET = "cdap.bq.source.view.materialization.dataset";
  String CONFIG_PARTITION_TYPE = "cdap.bq.sink.partition.type";
  String CONFIG_TIME_PARTITIONING_TYPE = "cdap.bq.sink.time.partitioning.type";
  String CONFIG_PARTITION_INTEGER_RANGE_START = "cdap.bq.sink.partition.integer.range.start";
  String CONFIG_PARTITION_INTEGER_RANGE_END = "cdap.bq.sink.partition.integer.range.end";
  String CONFIG_PARTITION_INTEGER_RANGE_INTERVAL = "cdap.bq.sink.partition.integer.range.interval";
  String CONFIG_TEMPORARY_TABLE_NAME = "cdap.bq.source.temporary.table.name";
  String CDAP_BQ_SINK_OUTPUT_SCHEMA = "cdap.bq.sink.output.schema";
  String BQ_FQN_PREFIX = "bigquery";
}
