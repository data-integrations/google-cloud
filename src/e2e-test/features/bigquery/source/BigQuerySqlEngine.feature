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

@BigQuery_Sink
Feature: BigQuery sink - Verification of BigQuery to BigQuery successful data transfer

  @BQ_SOURCE_JOINER_TEST @BQ_SOURCE_JOINER2_TEST @BQ_DELETE_JOIN @BQ_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario:Validate successful records transfer from BigQuery source to BigQuery sink using Join
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Joiner" from the plugins list as: "Analytics"
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
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "Joiner" to establish connection
    Then Connect plugins: "BigQuery2" and "Joiner" to establish connection
    Then Connect plugins: "Joiner" and "BigQuery3" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQRefName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable2"
    Then Validate "BigQuery2" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Joiner"
    Then Select radio button plugin property: "conditionType" with value: "basic"
    Then Click on the Get Schema button
    Then Validate "Joiner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery3"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Validate "BigQuery3" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Click on "Configure" button
    Then Click on "Transformation Pushdown" button
    Then Click on "Enable Transformation Pushdown" button
    Then Enter input plugin property: "dataset" with value: "test_sqlengine"
    Then Click on "Advanced" button
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Click on "Save" button
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "bqExpectedFileJoin"

  @BQ_SOURCE_SQLENGINE_TEST @BQ_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario:Validate successful records transfer from BigQuery source to BigQuery sink using group by
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Group By" from the plugins list as: "Analytics"
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
    Then Connect plugins: "BigQuery" and "Group By" to establish connection
    Then Connect plugins: "Group By" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "Group By"
    Then Select dropdown plugin property: "groupByFields" with option value: "groupByValidFirstField"
    Then Press Escape Key
    Then Select dropdown plugin property: "groupByFields" with option value: "groupByValidSecondField"
    Then Press Escape Key
    Then Enter GroupBy plugin Fields to be Aggregate "groupByGcsAggregateFields"
    Then Click on the Get Schema button
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
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
    Then Click on "Configure" button
    Then Click on "Transformation Pushdown" button
    Then Click on "Enable Transformation Pushdown" button
    Then Enter input plugin property: "dataset" with value: "test_sqlengine"
    Then Click on "Advanced" button
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Click on "Save" button
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "groupByTestOutputFile"

  @BQ_SOURCE_SQLENGINE_TEST @BQ_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario:Validate successful records transfer from BigQuery source to BigQuery sink using deduplicate
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    When Expand Plugin group in the LHS plugins list: "Analytics"
    When Select plugin: "Deduplicate" from the plugins list as: "Analytics"
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
    Then Connect plugins: "BigQuery" and "Deduplicate" to establish connection
    Then Connect plugins: "Deduplicate" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "Deduplicate"
    Then Select dropdown plugin property: "uniqueFields" with option value: "DeduplicateValidFirstField"
    Then Press Escape Key
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
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
    Then Click on "Configure" button
    Then Click on "Transformation Pushdown" button
    Then Click on "Enable Transformation Pushdown" button
    Then Enter input plugin property: "dataset" with value: "test_sqlengine"
    Then Click on "Advanced" button
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Click on "Save" button
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate The Data From BQ To BQ With Actual And Expected File for: "deduplicateTestOutputFile"
