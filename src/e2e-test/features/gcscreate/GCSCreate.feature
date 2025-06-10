@GCSCreate
Feature: GCSCreate - Verification of GCS Create plugin

  @GCS_CSV_TEST
  Scenario: Verify GCSCreate successfully creates objects in the GCS bucket
    Given Open Datafusion Project to configure pipelines
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Create" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Create"
    Then Enter the GCS Create property projectId "projectId"
    Then Enter the GCS Create property objects to create as path "gcsCreateObject1"
    Then Enter the GCS Create property objects to create as path "gcsCreateObject2"
    Then Select GCS Create property fail if objects exists as "true"
    Then Override Service account details if set in environment variables
    Then Enter GCSCreate property encryption key name "cmekGCS" if cmek is enabled
    Then Validate "GCS Create" plugin properties
    Then Close the GCS Create properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify that the object "gcsCreateObject1" created successfully
    Then Verify that the object "gcsCreateObject2" created successfully

  @GCS_CSV_TEST @GCSCreate_Required
  Scenario: Verify that pipeline with GCSCreate failed when fail if objects exists property set to true
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Create" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Create"
    Then Enter the GCS Create property projectId "projectId"
    Then Enter the GCS Create property objects to create as path "gcsCsvFile"
    Then Select GCS Create property fail if objects exists as "truhjhjjhjhe"
    Then Override Service account details if set in environment variables
    Then Validate "GCS Create" plugin properties
    Then Close the GCS Create properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Failed"
    Then Close the pipeline logs

  @GCS_CSV_TEST @GCSCreate_Required
  Scenario: Verify that pipeline with GCSCreate succeeded when fail if objects exists property set to false
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Create" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Create"
    Then Enter the GCS Create property projectId "projectId"
    Then Enter the GCS Create property objects to create as path "gcsCreateObject1"
    Then Select GCS Create property fail if objects exists as "false"
    Then Override Service account details if set in environment variables
    Then Validate "GCS Create" plugin properties
    Then Close the GCS Create properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
