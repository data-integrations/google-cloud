@GCSDelete
Feature: GCS Delete - Verification of GCS Delete plugin with macro arguments

  @GCS_CSV_TEST
  Scenario: Verify GCS Delete successfully deletes all objects in gcs bucket when bucket name is passed as macro
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Delete" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Delete"
    Then Enter the GCS Delete property "projectId" as macro argument "gcsDeleteProjectId"
    Then Enter the GCS Delete property "objectsToDelete" as macro argument "gcsDeleteObjectsToDelete"
    Then Enter the GCS Delete property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter the GCS Delete property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter the GCS Delete property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Validate "GCS Delete" plugin properties
    Then Close the GCS Delete properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "gcsDeleteProjectId"
    Then Enter runtime argument value source bucket name for key "gcsDeleteObjectsToDelete"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify all the objects in the GCS bucket deleted successfully by GCS Delete action plugin

  @GCS_READ_RECURSIVE_TEST
  Scenario: Verify GCS Delete successfully deletes all objects in gcs bucket when comma separated object list is passed as macro
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Delete" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Delete"
    Then Enter the GCS Delete property "projectId" as macro argument "gcsDeleteProjectId"
    Then Enter the GCS Delete property "objectsToDelete" as macro argument "gcsDeleteObjectsToDelete"
    Then Enter the GCS Delete property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter the GCS Delete property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter the GCS Delete property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Validate "GCS Delete" plugin properties
    Then Close the GCS Delete properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "gcsDeleteProjectId"
    Then Enter runtime argument value "gcsDeleteObjectsList" as comma separated objects for key "gcsDeleteObjectsToDelete"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify multiple objects "gcsDeleteObjectsList" deleted successfully by GCS Delete action plugin
