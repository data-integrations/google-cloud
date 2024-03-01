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
@BigTable @BIGTABLE_SOURCE_TEST
Feature: BigTable source - Verification of BigTable to BigTable Successful Data Transfer

  @BIGTABLE_SINK_TEST
  Scenario: To verify data is getting transferred from BigTable source table to BigTable sink table
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Bigtable" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Bigtable" from the plugins list as: "Sink"
    Then Connect plugins: "Bigtable" and "Bigtable2" to establish connection
    Then Navigate to the properties page of plugin: "Bigtable"
    Then Enter input plugin property: "referenceName" with value: "CBTSourceReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableInstance"
    Then Enter input plugin property: "table" with value: "bigtableSourceTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsourceMappings"
    Then Select Macro action of output schema property: "outputSchemaMacroInput" and set the value to "macroKeyString"
    Then Validate "Bigtable" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Bigtable2"
    Then Enter input plugin property: "referenceName" with value: "CBTSinkReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableTargetInstance"
    Then Enter input plugin property: "table" with value: "bigtableTargetTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsinkMappings"
    Then Validate "Bigtable" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "cbtSourceOutputSchema" for key "macroKeyString"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Validate data transferred to target bigtable table with data of source bigtable table

  @EXISTING_BIGTABLE_SINK
  Scenario: To verify data is getting transferred from BigTable source table to existing BigTable sink
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Bigtable" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Bigtable" from the plugins list as: "Sink"
    Then Connect plugins: "Bigtable" and "Bigtable2" to establish connection
    Then Navigate to the properties page of plugin: "Bigtable"
    Then Enter input plugin property: "referenceName" with value: "CBTSourceReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableInstance"
    Then Enter input plugin property: "table" with value: "bigtableSourceTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsourceMappings"
    Then Select Macro action of output schema property: "outputSchemaMacroInput" and set the value to "macroKeyString"
    Then Validate "Bigtable" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Bigtable2"
    Then Enter input plugin property: "referenceName" with value: "CBTSinkReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableTargetInstance"
    Then Enter input plugin property: "table" with value: "bigtableTargetExistingTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsinkMappings"
    Then Validate "Bigtable" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "cbtSourceOutputSchema" for key "macroKeyString"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Validate data transferred to existing target bigtable table with data of source bigtable table

  @BIGTABLE_SINK_TEST
  Scenario: To verify data is getting transferred from unvalidated BigTable source table to BigTable sink table
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Bigtable" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Bigtable" from the plugins list as: "Sink"
    Then Connect plugins: "Bigtable" and "Bigtable2" to establish connection
    Then Navigate to the properties page of plugin: "Bigtable"
    Then Enter input plugin property: "referenceName" with value: "CBTSourceReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableInstance"
    Then Enter input plugin property: "table" with value: "bigtableSourceTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsourceMappings"
    Then Select Macro action of output schema property: "outputSchemaMacroInput" and set the value to "macroKeyString"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Bigtable2"
    Then Enter input plugin property: "referenceName" with value: "CBTSinkReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableTargetInstance"
    Then Enter input plugin property: "table" with value: "bigtableTargetTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsinkMappings"
    Then Validate "Bigtable" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "cbtSourceOutputSchema" for key "macroKeyString"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Validate data transferred to target bigtable table with data of source bigtable table
