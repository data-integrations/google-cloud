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
Feature: GCSCopy - Validate GCSCopy plugin error scenarios

  @GCSCopy_Required @ITN_TEST
  Scenario:Verify GCSCopy plugin properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Copy" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Copy"
    Then Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | sourcePath |
      | destPath   |

  @GCS_SINK_TEST @GCSCopy_Required @ITN_TEST
  Scenario:Verify GCSCopy plugin error message for invalid bucket name in Source Path
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Copy" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Copy"
    And Enter input plugin property: "sourcePath" with value: "invalidsourcePath"
    And Enter input plugin property: "destPath" with value: "gcsTargetBucketName"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "sourcePath" is displaying an in-line error message: "errorMessageInvalidSourcePath"

  @GCS_CSV_TEST @GCSCopy_Required @ITN_TEST
  Scenario:Verify GCSCopy plugin error message for invalid bucket name in Destination Path
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Copy" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Copy"
    And Enter input plugin property: "sourcePath" with value: "gcsCsvFile"
    And Enter input plugin property: "destPath" with value: "invaliddestPath"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "destPath" is displaying an in-line error message: "errorMessageInvalidDestPath"

  @GCS_CSV_TEST @GCS_SINK_TEST @GCSCopy_Required @ITN_TEST
  Scenario:Verify GCSCopy plugin error message for invalid Encryption Key Name
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Copy" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Copy"
    And Enter input plugin property: "sourcePath" with value: "gcsCsvFile"
    And Enter input plugin property: "destPath" with value: "gcsTargetBucketName"
    And Enter input plugin property: "cmekKey" with value: "invalidEncryptionKey"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "cmekKey" is displaying an in-line error message: "errorMessageInvalidEncryptionKey"
