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
Feature:GCSCopy - Verification of successful objects copy from one bucket to another with macro arguments

  @CMEK @GCS_CSV_TEST @GCS_SINK_TEST
  Scenario:Validate successful copy object from one bucket to another new bucket with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Copy" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Copy"
    Then Click on the Macro button of Property: "project" and set the value to: "projectId"
    Then Click on the Macro button of Property: "sourcePath" and set the value to: "SourcePath"
    Then Click on the Macro button of Property: "destPath" and set the value to: "DestPath"
    Then Override Service account details if set in environment variables
    Then Enter GCSCopy property encryption key name "cmekGCS" if cmek is enabled
    Then Validate "GCS Copy" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "projectId"
    Then Enter runtime argument value "gcsCsvFile" for GCSCopy property sourcePath key "SourcePath"
    Then Enter runtime argument value for GCSCopy property destination path key "DestPath"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate GCSCopy successfully copies object "gcsCsvFile" to destination bucket
