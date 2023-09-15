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

  @BQ_SOURCE_DATATYPE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with partition type TIME  with Partition field and require partitioned filter true
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "projectId"
    And Replace input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "table" with value: "bqSourceTable"
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
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "updateTableSchema"
    Then Enter BigQuery sink property partition field "bqPartitionFieldTime"
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
    Then Verify the pipeline status is "Succeeded"
    Then Verify the partition table is created with partitioned on field "bqPartitionFieldTime"

  @BQ_INSERT_SOURCE_TEST @BQ_UPDATE_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with Advanced Operations Update for table key.
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
    And Select radio button plugin property: "operation" with value: "update"
    Then Click plugin property: "updateTableSchema"
    Then Click on the Add Button of the property: "relationTableKey" with value:
      | TableKey |
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
    Then Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table

  @BQ_INSERT_SOURCE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with Advanced operations Upsert
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
    And Select radio button plugin property: "operation" with value: "upsert"
    Then Click plugin property: "updateTableSchema"
    Then Click on the Add Button of the property: "relationTableKey" with value:
      | TableKey |
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
    Then Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table

  @BQ_SOURCE_DATATYPE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with clustering order functionality
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
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
    Then Enter BigQuery sink property partition field "bqPartitionFieldTime"
    Then Click on the Add Button of the property: "clusteringOrder" with value:
      | clusterValue |
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
    Then Verify the pipeline status is "Succeeded"
    Then Verify the partition table is created with partitioned on field "bqPartitionFieldTime"

  @BQ_SOURCE_DATATYPE_TEST @BQ_EXISTING_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario: Validate user is able to read the records from BigQuery source(existing table),source table here has more columns than BigQuery sink(existing table) with update button schema and truncate table to true with use connection functionality
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
    Then Click on the Browse button inside plugin properties
    Then Select connection data row with name: "dataset"
    Then Select connection data row with name: "bqSourceTable"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Verify input plugin property: "table" contains value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Click on the Browse button inside plugin properties
    Then Click SELECT button inside connection data row with name: "dataset"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "updateTableSchema"
    Then Click plugin property: "truncateTable"
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
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table

  @BQ_INSERT_SOURCE_TEST @BQ_UPDATE_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario:Validate successful records transfer from BigQuery to BigQuery with Advanced Operations Update without updating the destination table schema with use connection functionality
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
    Then Click on the Browse button inside plugin properties
    Then Select connection data row with name: "dataset"
    Then Select connection data row with name: "bqSourceTable"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Verify input plugin property: "table" contains value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Click on the Browse button inside plugin properties
    Then Click SELECT button inside connection data row with name: "dataset"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    And Select radio button plugin property: "operation" with value: "update"
    Then Click on the Add Button of the property: "relationTableKey" with value:
      | TableKey |
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
    Then Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table

  @BQ_INSERT_SOURCE_TEST @BQ_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario:Validate successful records transfer from BigQuery to BigQuery with Advanced operations Upsert without updating the destination table schema with use connection functionality
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
    Then Click on the Browse button inside plugin properties
    Then Select connection data row with name: "dataset"
    Then Select connection data row with name: "bqSourceTable"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Verify input plugin property: "table" contains value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Click on the Browse button inside plugin properties
    Then Click SELECT button inside connection data row with name: "dataset"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    And Select radio button plugin property: "operation" with value: "upsert"
    Then Click on the Add Button of the property: "relationTableKey" with value:
      | TableKey |
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
    Then Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table
