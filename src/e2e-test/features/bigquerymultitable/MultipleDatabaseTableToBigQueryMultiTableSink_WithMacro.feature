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

@BQMT_SINK
Feature: BigQueryMultiTable sink -Verification of Multiple Database Tables to BigQueryMultiTable successful data transfer using macros

  @MULTIPLEDATABASETABLE_SOURCE_TEST @BQMT_Required @PLUGIN-1669
  Scenario:Verify data is getting transferred from Multiple Database Tables to BQMT sink with all datatypes using macros
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "Multiple Database Tables" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery Multi Table" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "Multiple Database Tables"
    Then Replace input plugin property: "referenceName" with value: "ref"
    Then Enter input plugin property: "connectionString" with value: "connectionString" for Credentials and Authorization related fields
    Then Replace input plugin property: "jdbcPluginName" with value: "mysql"
    Then Replace input plugin property: "user" with value: "user" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "pass" for Credentials and Authorization related fields
    And Select radio button plugin property: "dataSelectionMode" with value: "sql-statements"
    Then Click on the Add Button of the property: "sqlStatements" with value:
      | selectQuery|
    Then Validate "Multiple Database Tables" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Enter input plugin property: "referenceName" with value: "Reference"
    Then Click on the Macro button of Property: "projectId" and set the value to: "bqProjectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "bqDatasetProjectId"
    Then Click on the Macro button of Property: "serviceAccountType" and set the value to: "serviceAccountType"
    Then Click on the Macro button of Property: "serviceAccountFilePath" and set the value to: "serviceAccount"
    Then Click on the Macro button of Property: "dataset" and set the value to: "bqDataset"
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "allowSchema"
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    Then Connect plugins: "Multiple Database Tables" and "BigQuery Multi Table" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to BQMT sink is equal to the value from source MultiDatabase table

  @MULTIPLEDATABASETABLE_SOURCE_TEST @BQMT_Required @PLUGIN-1669
  Scenario:Verify data is getting transferred from Multiple Database Tables to BQMT sink with split field using macros
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "Multiple Database Tables" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery Multi Table" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "Multiple Database Tables"
    Then Replace input plugin property: "referenceName" with value: "ref"
    Then Enter input plugin property: "connectionString" with value: "connectionString" for Credentials and Authorization related fields
    Then Replace input plugin property: "jdbcPluginName" with value: "mysql"
    Then Replace input plugin property: "user" with value: "user" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "pass" for Credentials and Authorization related fields
    And Select radio button plugin property: "dataSelectionMode" with value: "sql-statements"
    Then Click on the Add Button of the property: "sqlStatements" with value:
      | selectQuery|
    Then Validate "Multiple Database Tables" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Enter input plugin property: "referenceName" with value: "Reference"
    Then Click on the Macro button of Property: "projectId" and set the value to: "bqProjectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "bqDatasetProjectId"
    Then Click on the Macro button of Property: "serviceAccountType" and set the value to: "serviceAccountType"
    Then Click on the Macro button of Property: "serviceAccountFilePath" and set the value to: "serviceAccount"
    Then Click on the Macro button of Property: "dataset" and set the value to: "bqDataset"
    Then Click on the Macro button of Property: "SplitField" and set the value to: "bqmtSplitField"
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "allowSchema"
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    Then Connect plugins: "Multiple Database Tables" and "BigQuery Multi Table" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "splitField" for key "bqmtSplitField"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "splitField" for key "bqmtSplitField"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to BQMT sink is equal to the value from source MultiDatabase table
