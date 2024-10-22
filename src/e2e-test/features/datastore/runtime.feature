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

@DataStores
Feature: DataStore - Verification of Datastore to Datastore Successful Data Transfer

  @DATASTORE_SOURCE_ENTITY @datastore_Required
  Scenario: To verify data is getting transferred from Datastore to Datastore successfully using filter and custom index
    Given Open Datafusion Project to configure pipeline
    Then Select plugin: "Datastore" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Datastore"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "ReferenceName"
    Then Enter key value pairs for plugin property: "filters" with values from json: "filterOptions"
    Then Enter kind for datastore plugin
    Then Select dropdown plugin property: "keyType" with option value: "None"
    Then Click on the Get Schema button
    Then Validate "Datastore" plugin properties
    Then Close the Plugin Properties page
    And Select Sink plugin: "Datastore" from the plugins list
    Then Connect plugins: "Datastore" and "Datastore2" to establish connection
    Then Navigate to the properties page of plugin: "Datastore2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Enter kind for datastore plugin
    Then Select dropdown plugin property: "indexStrategy" with option value: "Custom"
    Then Enter Value for plugin property table key : "indexedProperties" with values: "propertyName"
    Then Validate "datastore2" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From Datastore To Datastore With Actual And Expected File for: "dsExpectedFile"

  @DATASTORE_SOURCE_ENTITY @datastore_Required
  Scenario: To verify data is getting transferred from Datastore to Datastore using Urlsafekey
    Given Open Datafusion Project to configure pipeline
    Then Select plugin: "Datastore" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Datastore"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "ReferenceName"
    Then Enter key value pairs for plugin property: "filters" with values from json: "filterOptions"
    Then Enter kind for datastore plugin
    Then Select dropdown plugin property: "keyType" with option value: "URL-safe key"
    Then Enter input plugin property: "keyAlias" with value: "fieldName"
    Then Click on the Get Schema button
    Then Validate "Datastore" plugin properties
    Then Close the Plugin Properties page
    And Select Sink plugin: "Datastore" from the plugins list
    Then Connect plugins: "Datastore" and "Datastore2" to establish connection
    Then Navigate to the properties page of plugin: "Datastore2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Select dropdown plugin property: "keyType" with option value: "URL-safe key"
    Then Enter input plugin property: "keyAlias" with value: "fieldName"
    Then Enter kind for datastore plugin
    Then Enter Ancestor for the datastore plugin
    Then Validate "datastore2" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From Datastore To Datastore With Actual And Expected File for: "dsExpectedFile"

  @DATASTORE_SOURCE_ENTITY @datastore_Required
  Scenario: To verify data is getting transferred from Datastore to Datastore using Ancestor and Key Literal
    Given Open Datafusion Project to configure pipeline
    Then Select plugin: "Datastore" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Datastore"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "ReferenceName"
    Then Enter kind for datastore plugin
    Then Enter Ancestor for the datastore plugin
    Then Select dropdown plugin property: "keyType" with option value: "Key literal"
    Then Enter input plugin property: "keyAlias" with value: "fieldName"
    Then Click on the Get Schema button
    Then Validate "Datastore" plugin properties
    Then Close the Plugin Properties page
    And Select Sink plugin: "Datastore" from the plugins list
    Then Connect plugins: "Datastore" and "Datastore2" to establish connection
    Then Navigate to the properties page of plugin: "Datastore2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Select dropdown plugin property: "keyType" with option value: "Key literal"
    Then Enter input plugin property: "keyAlias" with value: "fieldName"
    Then Enter kind for datastore plugin
    Then Enter Ancestor for the datastore plugin
    Then Validate "datastore2" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From Datastore To Datastore With Actual And Expected File for: "dsExpectedFile"

  @DATASTORE_SOURCE_ENTITY @datastore_Required
  Scenario: To verify data is getting transferred from Datastore to Datastore using Ancestor and Custom Key
    Given Open Datafusion Project to configure pipeline
    Then Select plugin: "Datastore" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Datastore"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "ReferenceName"
    Then Enter kind for datastore plugin
    Then Enter Ancestor for the datastore plugin
    Then Select dropdown plugin property: "keyType" with option value: "Key literal"
    Then Enter input plugin property: "keyAlias" with value: "fieldName"
    Then Click on the Get Schema button
    Then Validate "Datastore" plugin properties
    Then Close the Plugin Properties page
    And Select Sink plugin: "Datastore" from the plugins list
    Then Connect plugins: "Datastore" and "Datastore2" to establish connection
    Then Navigate to the properties page of plugin: "Datastore2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "refName"
    Then Select dropdown plugin property: "keyType" with option value: "Custom name"
    Then Enter input plugin property: "keyAlias" with value: "fieldName"
    Then Enter kind for datastore plugin
    Then Validate "datastore2" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate The Data From Datastore To Datastore With Actual And Expected File for: "dsExpectedFile"
