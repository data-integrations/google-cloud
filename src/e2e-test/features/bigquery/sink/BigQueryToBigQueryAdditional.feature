# Copyright © 2023 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

@BigQuery_Sink
Feature: BigQuery sink - Verification of BigQuery to BigQuery successful data transfer

  @BQ_UPSERT_SOURCE_TEST @BQ_UPSERT_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario:Validate successful records transfer from BigQuery source to BigQuery sink with Upsert operation by updating destination table schema and destination table exists with records in it.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    And Select radio button plugin property: "operation" with value: "upsert"
    Then Click on the Add Button of the property: "relationTableKey" with value:
      | TableKeyUpsert |
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from BigQuery to BigQuery with actual And expected file for: "bqUpsertExpectedFile"

  @BQ_NULL_MODE_SOURCE_TEST @BQ_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario: Validate Successful record transfer from BigQuery source plugin to BigQuery sink plugin having all null values in one column and few null values in another column in Source table
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table

  @BQ_UPDATE_SOURCE_DEDUPE_TEST @BQ_UPDATE_SINK_DEDUPE_TEST @EXISTING_BQ_CONNECTION
  Scenario: Verify successful record transfer from BigQuery source to BigQuery sink using advance operation update with Dedupe By Property.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    And Select radio button plugin property: "operation" with value: "update"
    Then Enter Value for plugin property table key : "relationTableKey" with values: "relationTableKeyValue"
    Then Select dropdown plugin property: "dedupeBy" with option value: "dedupeByOrder"
    Then Enter key for plugin property: "dedupeBy" with values: "dedupeByValue"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from BigQuery to BigQuery with actual And expected file for: "bqUpdateDedupeExpectedFile"

  @BQ_INSERT_INT_SOURCE_TEST @BQ_EXISTING_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario: Verify successful record transfer for the Insert operation with partition type Integer and destination table is existing already.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Select BigQuery sink property partitioning type as "INTEGER"
    Then Enter input plugin property: "rangeStart" with value: "rangeStartValue"
    Then Enter input plugin property: "rangeEnd" with value: "rangeEndValue"
    Then Enter input plugin property: "rangeInterval" with value: "rangeIntervalValue"
    Then Enter input plugin property: "partitionByField" with value: "partitionByFieldValue"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from BigQuery to BigQuery with actual And expected file for: "bqInsertExpectedFile"

  @BQ_TIME_SOURCE_TEST @BQ_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario: Verify successful record transfer for the Insert operation from BigQuery source plugin to BigQuery sink with partition type Time and partition field is date.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Enter input plugin property: "partitionByField" with value: "bqPartitionFieldDate"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from BigQuery to BigQuery with actual And expected file for: "bqDateExpectedFile"

  @BQ_TIME_SOURCE_TEST @BQ_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario: Verify successful record transfer for the Insert operation from BigQuery source plugin to BigQuery sink with partition type Time and partition field is datetime.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Enter input plugin property: "partitionByField" with value: "bqPartitionFieldDateTime"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from BigQuery to BigQuery with actual And expected file for: "bqDateTimeExpectedFile"

  @BQ_TIME_SOURCE_TEST @BQ_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario: Verify successful record transfer for the Insert operation from BigQuery source plugin to BigQuery sink with partition type Time and partition field is timestamp.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Enter input plugin property: "partitionByField" with value: "bqPartitionFieldTimeStamp"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from BigQuery to BigQuery with actual And expected file for: "bqTimeStampExpectedFile"

  @BQ_UPSERT_DEDUPE_SOURCE_TEST @BQ_UPSERT_DEDUPE_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario:Validate successful records transfer from BigQuery source to BigQuery sink with Upsert operation with dedupe source data and existing destination table where update table schema is set to false
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    And Select radio button plugin property: "operation" with value: "upsert"
    Then Click on the Add Button of the property: "relationTableKey" with value:
      | TableKeyDedupe |
    Then Select dropdown plugin property: "dedupeBy" with option value: "dedupeBy"
    Then Enter key for plugin property: "dedupeBy" with values: "dedupeByValueUpsert"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data transferred from BigQuery to BigQuery with actual And expected file for: "bqUpsertDedupeFile"

  @BQ_PRIMARY_RECORD_SOURCE_TEST @BQ_SECONDARY_RECORD_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Validate successful record transfer from two BigQuery source plugins with different schema record names, taking one extra column in BigQuery source plugin 1,and
  using wrangler transformation plugin for removing the extra column and transferring the data in BigQuery sink plugin containing all the columns from both the source plugin.
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the file for importing the pipeline for the plugin "Directive_Drop"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable2"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery3"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the data transferred from BigQuery to BigQuery with actual And expected file for: "bqDifferentRecordFile"

  @BQ_INSERT_INT_SOURCE_TEST @BQ_INSERT_SINK_TEST @CDAP-20830
  Scenario:Validate successful records transfer from BigQuery to BigQuery with Advanced operations Insert with table existing in both source and sink plugin and update table schema to true.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    And Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    And Replace input plugin property: "datasetProject" with value: "datasetprojectId"
    And Replace input plugin property: "referenceName" with value: "reference"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    And Select radio button plugin property: "operation" with value: "insert"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
