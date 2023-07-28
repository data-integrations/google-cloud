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
Feature: BigQueryMultiTable sink - Validate BigQueryMultiTable sink plugin error scenarios

  @BQMT_Required
  Scenario Outline: Verify BigQueryMultiTable Sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQueryMultiTable" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    Then Click on the Validate button
    Then Validate mandatory property error for "<property>"
    Examples:
      | property |
      | dataset  |

  Scenario:Verify BQMT Sink properties validation errors for incorrect value of chunk size
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQueryMultiTable" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "gcsChunkSize" with value: "bqmtInvalidChunkSize"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "gcsChunkSize" is displaying an in-line error message: "errorMessageIncorrectBQMTChunkSize"

  @BQMT_Required
  Scenario:Verify BQMT Sink properties validation errors for incorrect dataset
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQueryMultiTable" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "dataset" with value: "bqmtInvalidSinkDataset"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "dataset" is displaying an in-line error message: "errorMessageIncorrectBQMTDataset"

  Scenario:Verify BQMT Sink properties validation errors for incorrect reference name
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQueryMultiTable" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "bqmtInvalidSinkReferenceName"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "referenceName" is displaying an in-line error message: "errorMessageIncorrectBQMTReferenceName"

  Scenario:Verify BQMT Sink properties validation errors for incorrect value of temporary bucket name
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQueryMultiTable" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "bucket" with value: "bqmtInvalidTemporaryBucket"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "bucket" is displaying an in-line error message: "errorMessageIncorrectBQMTBucketName"
