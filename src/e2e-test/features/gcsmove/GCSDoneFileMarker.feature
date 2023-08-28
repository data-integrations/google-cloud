@GCSMove
Feature:GCSDoneFileMarker - Validate GCSDoneFileMarker scenarios with GCSMove pipeline

  @CMEK @GCS_CSV_TEST @GCS_SINK_TEST
  Scenario:Validate GCSDoneFileMarker SUCCESS run condition with GCSMove pipeline
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Move" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Move"
    Then Enter GCSMove property projectId "projectId"
    Then Enter GCSMove property source path
    Then Enter GCSMove property destination path
    Then Select GCSMove property move all subdirectories as "true"
    Then Override Service account details if set in environment variables
    Then Validate "GCSMove" plugin properties
    Then Close GCSMove properties
    Then Open pipeline alerts from pipeline configure
    Then Open GCSDoneFileMarker to configure
    Then Enter GCSMove property projectId "projectId"
    Then Select GCSDoneFileMarker property Run condition as "success"
    Then Enter GCSDoneFileMarker destination path with "gcsDoneSuccessFile"
    Then Confirm GCSDoneFileMarker configuration
    Then Save Pipeline alerts and close
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify whether the "gcsDoneSuccessFile" object present in destination bucket

  @CMEK @GCS_CSV_TEST @GCS_SINK_TEST
  Scenario:Validate GCSDoneFileMarker FAILURE run condition with GCSMove pipeline
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Move" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Move"
    Then Enter GCSMove property projectId "projectId"
    Then Enter input plugin property: "sourcePath" with value: "wrongSourcePath"
    Then Enter GCSMove property destination path
    Then Select GCSMove property move all subdirectories as "true"
    Then Override Service account details if set in environment variables
    Then Validate "GCSMove" plugin properties
    Then Close GCSMove properties
    Then Open pipeline alerts from pipeline configure
    Then Open GCSDoneFileMarker to configure
    Then Enter GCSMove property projectId "projectId"
    Then Select GCSDoneFileMarker property Run condition as "failure"
    Then Enter GCSDoneFileMarker destination path with "gcsDoneFailedFile"
    Then Confirm GCSDoneFileMarker configuration
    Then Save Pipeline alerts and close
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Failed"
    Then Verify whether the "gcsDoneFailedFile" object present in destination bucket
