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

@GCSCopy
Feature:GCSCopy - Verification of successful objects copy from one bucket to another

  @CMEK @GCS_CSV_TEST @GCS_SINK_TEST
  Scenario:Validate successful copy object from one bucket to another new bucket along with data validation with default subdirectory and overwrite toggle button as false.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Copy" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Copy"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter GCSCopy property source path "gcsCsvFile"
    And Enter GCSCopy property destination path
    Then Override Service account details if set in environment variables
    Then Enter GCSCopy property encryption key name "cmekGCS" if cmek is enabled
    Then Validate "GCS Copy" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate GCSCopy successfully copies object "gcsCsvFile" to destination bucket
    Then Validate the data of GCS Copy source bucket and destination bucket "gcsCopyCsvExpectedFilePath"

  @GCS_READ_RECURSIVE_TEST @GCS_SINK_TEST @GCSCopy_Required
    Scenario: Validate successful copy objects from one bucket to another with Copy All Subdirectories set to true along with data validation.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Copy" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Copy"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter GCSCopy property source path "gcsCopyReadRecursivePath"
    And Enter GCSCopy property destination path
    Then Override Service account details if set in environment variables
    Then Enter GCSCopy property encryption key name "cmekGCS" if cmek is enabled
    Then Select radio button plugin property: "recursive" with value: "true"
    Then Validate "GCS Copy" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate GCSCopy copies subdirectories along with its files to the destination bucket

  @GCS_READ_RECURSIVE_TEST @GCS_SINK_TEST @GCSCopy_Required
  Scenario: Validate successful copy objects from one bucket to another with Copy All Subdirectories set to false along with data validation.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Copy" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Copy"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter GCSCopy property source path "gcsCopyReadRecursivePath"
    And Enter GCSCopy property destination path
    Then Override Service account details if set in environment variables
    Then Enter GCSCopy property encryption key name "cmekGCS" if cmek is enabled
    Then Select radio button plugin property: "recursive" with value: "false"
    Then Validate "GCS Copy" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate GCSCopy did not copy subdirectories along with its files to the destination bucket

  @GCS_CSV_TEST @GCS_SINK_EXISTING_BUCKET_TEST @GCSCopy_Required
    Scenario: Validate successful copy objects from one bucket to another existing bucket with Overwrite Existing Files set to true along with data validation.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Copy" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Copy"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter GCSCopy property source path "gcsCsvFile"
    Then Enter GCSCopy property destination path "gcsCopyReadRecursivePath"
    Then Override Service account details if set in environment variables
    Then Enter GCSCopy property encryption key name "cmekGCS" if cmek is enabled
    Then Select radio button plugin property: "overwrite" with value: "true"
    Then Validate "GCS Copy" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate GCSCopy successfully copies object "gcsCsvFile" to destination bucket
    Then Validate the data of GCS Copy source bucket and destination bucket "gcsCopyCsvExpectedFilePath"

  @GCS_CSV_TEST @GCS_SINK_EXISTING_BUCKET_TEST
  Scenario: Validate successful copy objects from one bucket to another existing bucket with Overwrite Existing Files set to false along with data validation.
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Copy" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Copy"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter GCSCopy property source path "gcsCsvFile"
    Then Enter GCSCopy property destination path "gcsCopyReadRecursivePath"
    Then Override Service account details if set in environment variables
    Then Enter GCSCopy property encryption key name "cmekGCS" if cmek is enabled
    Then Select radio button plugin property: "overwrite" with value: "false"
    Then Validate "GCS Copy" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    And Verify the pipeline status is "Failed"
    Then Close the pipeline logs
    Then Validate GCSCopy failed to copy object "gcsCsvFile" to destination bucket
    Then Validate the data of GCS Copy source bucket and destination bucket "gcsCopyCsvExpectedFilePath"

  @GCS_CSV_TEST @GCS_SINK_TEST
  Scenario:Validate successful Copy object from one bucket to another new bucket with location set to non-default value
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Copy" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Copy"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter GCSCopy property source path "gcsCsvFile"
    And Enter GCSCopy property destination path
    Then Override Service account details if set in environment variables
    Then Replace input plugin property: "location" with value: "locationEU"
    Then Enter GCSCopy property encryption key name "cmekGCS" if cmek is enabled
    Then Validate "GCS Copy" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate GCSCopy successfully copied object "gcsCsvFile" to destination bucket in location "locationEU"
    Then Validate the data of GCS Copy source bucket and destination bucket "gcsCopyCsvExpectedFilePath"