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
  String CONFIG_DESTINATION_TABLE_EXISTS = "cdap.bq.sink.destination.table.exists";
  String CONFIG_CREATE_PARTITIONED_TABLE = "cdap.bq.sink.create.partitioned.table";
  String CONFIG_PARTITION_BY_FIELD = "cdap.bq.sink.partition.by.field";
  String CONFIG_REQUIRE_PARTITION_FILTER = "cdap.bq.sink.require.partition.filter";
  String CONFIG_PARTITION_FROM_DATE = "cdap.bq.source.partition.from.date";
  String CONFIG_PARTITION_TO_DATE = "cdap.bq.source.partition.to.date";
  String CONFIG_SERVICE_ACCOUNT_FILE_PATH = "cdap.bq.service.account.file.path";
  String CONFIG_CLUSTERING_ORDER = "cdap.bq.sink.clustering.order";
  String CONFIG_OPERATION = "cdap.bq.sink.operation";
  String CONFIG_TABLE_KEY = "cdap.bq.sink.table.key";
  String CONFIG_DEDUPE_BY = "cdap.bq.sink.dedupe.by";
  String CONFIG_TABLE_FIELDS = "cdap.bq.sink.table.fields";
  String CONFIG_FILTER = "cdap.bq.source.filter";
  String CONFIG_PARTITION_FILTER = "cdap.bq.sink.partition.filter";
  String CONFIG_JOB_ID = "cdap.bq.sink.job.id";
}
