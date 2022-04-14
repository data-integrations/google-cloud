@GCSMove
Feature:GCSMove - Verification of successful objects move from one bucket to another with macro arguments

  @CMEK @GCS_CSV_TEST @GCS_SINK_TEST
  Scenario:Validate successful move object from one bucket to another new bucket with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Move" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Move"
    Then Enter GCSMove property "projectId" as macro argument "gcsMoveProjectId"
    Then Enter GCSMove property "gcsMoveSourcePath" as macro argument "gcsMoveSourcePath"
    Then Enter GCSMove property "gcsMoveDestinationPath" as macro argument "gcsMoveDestinationPath"
    Then Enter GCSMove property "serviceAccountFilePath" as macro argument "gcsMoveServiceAccount"
    Then Enter GCSMove cmek property "encryptionKeyName" as macro argument "cmekGCS" if cmek is enabled
    Then Validate "GCSMove" plugin properties
    Then Close GCSMove properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "gcsMoveProjectId"
    Then Enter runtime argument value "gcsCsvFile" for GCSMove property sourcePath key "gcsMoveSourcePath"
    Then Enter runtime argument value for GCSMove property destination path key "gcsMoveDestinationPath"
    Then Enter runtime argument value "serviceAccountAutoDetect" for key "gcsMoveServiceAccount"
    Then Enter runtime argument value "cmekGCS" for GCSMove cmek property key "cmekGCS" if GCS cmek is enabled
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate GCSMove successfully moved object "gcsCsvFile" to destination bucket
    Then Validate the cmek key "cmekGCS" of target GCS bucket if cmek is enabled
