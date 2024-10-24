@GCSCreate
Feature: GCSCreate - Verification of GCS Create plugin with macro arguments

  @GCS_CSV_TEST
  Scenario: Verify GCSCreate creates objects in the GCS bucket with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Create" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Create"
    Then Enter GCSCreate property "projectId" as macro argument "gcsCreateProjectId"
    Then Enter GCSCreate property "objectsToCreate" as macro argument "gcsCreateObjectsToCreate"
    Then Enter GCSCreate property "createFailIfObjectExists" as macro argument "gcsCreateFailIfObjectExists"
    Then Enter GCSCreate property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter GCSCreate property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter GCSCreate property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter GCSCreate cmek property "encryptionKeyName" as macro argument "cmekGCS" if cmek is enabled
    Then Validate "GCS Create" plugin properties
    Then Close the GCS Create properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtimes
    Then Enter runtime argument value "projectId" for key "gcsCreateProjectId"
    Then Enter runtime argument value "gcsCreateObject1" for GCSCreate property key "gcsCreateObjectsToCreate"
    Then Enter runtime argument value "true" for GCSCreate property key "gcsCreateFailIfObjectExists"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify that the object "gcsCreateObject1" created successfully
