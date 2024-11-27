# Copyright © 2024 Cask Data, Inc.
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

@BigQueryMultiTable_Sink
Feature: BigQueryMultiTable sink -Verification of BigQuery to BigQueryMultiTable successful data transfer

  @BQ_TWO_SOURCE_BQMT_TEST @BQ_TWO_SINK_BQMT_TEST
  Scenario: Verify data successfully transferred from BigQuery To BigQueryMultiTable in two new tables
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable2"
    Then Click on the Get Schema button
    Then Validate "BigQuery2" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery Multi Table" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery Multi Table" to establish connection
    Then Connect plugins: "BigQuery2" and "BigQuery Multi Table" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    Then Click plugin property: "flexibleSchema"
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate data transferred from BigQuery To BigQueryMultiTable is equal

  @BQ_SINGLE_SOURCE_BQMT_TEST @BQ_SINK_BQMT_TEST
  Scenario: Verify data successfully transferred from BigQuery To BigQueryMultiTable in one table
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery Multi Table" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery Multi Table" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    Then Click plugin property: "flexibleSchema"
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate data transferred from BigQuery To BigQueryMultiTable in one table is equal

  @BQ_TWO_SOURCE_BQMT_TEST @BQ_EXISTING_TARGET_TEST @BQ_TWO_SINK_BQMT_TEST
  Scenario: Verify data successfully transferred from BigQuery To BigQueryMultiTable in two existing tables
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable2"
    Then Click on the Get Schema button
    Then Validate "BigQuery2" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery Multi Table" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery Multi Table" to establish connection
    Then Connect plugins: "BigQuery2" and "BigQuery Multi Table" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    Then Click plugin property: "flexibleSchema"
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate data transferred from BigQuery To BigQueryMultiTable is equal

  @BQ_TWO_SOURCE_BQMT_TEST @BQ_EXISTING_TARGET_TEST @BQ_TWO_SINK_BQMT_TEST
  Scenario: Verify data successfully transferred from BigQuery To BigQueryMultiTable in two existing tables using truncate
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable2"
    Then Click on the Get Schema button
    Then Validate "BigQuery2" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery Multi Table" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery Multi Table" to establish connection
    Then Connect plugins: "BigQuery2" and "BigQuery Multi Table" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    Then Click plugin property: "flexibleSchema"
    Then Toggle BigQuery sink property truncateTable to true
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate data transferred from BigQuery To BigQueryMultiTable is equal

  @BQ_SOURCE_UPDATE_TEST @BQ_EXISTING_TARGET_TEST @BQ_TWO_SINK_BQMT_TEST
  Scenario: Verify data successfully transferred from BigQuery To BigQueryMultiTable in two existing tables after updating schema
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "dataset" with value: "dataset"
    Then Replace input plugin property: "table" with value: "bqSourceTable2"
    Then Click on the Get Schema button
    Then Validate "BigQuery2" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery Multi Table" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery Multi Table" to establish connection
    Then Connect plugins: "BigQuery2" and "BigQuery Multi Table" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    Then Click plugin property: "flexibleSchema"
    Then Select radio button plugin property: "updateSchema" with value: "true"
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate data transferred from BigQuery To BigQueryMultiTable is equal
