@GCSMove
Feature: GCSMove - Validate GCSMove plugin error scenarios

  Scenario Outline:Verify GCSMove plugin properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Move" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GsCS Move"
    Then Enter GCS Move properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property   |
      | sourcePath |
      | destPath   |

  @GCS_SINK_TEST
  Scenario:Verify GCSMove plugin error message for invalid bucket name in Source Path
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Move" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plsugin: "GCS Move"
    Then Enter GCSMove property source path "gcsInvalidBucketName"
    Then Enter GCSMove property destination path
    Then Verify GCS Move property "sourcePath" invalid bucket name error message is displayed for bucket "gcsInvalidBucketName"

  @GCS_CSV_TEST
  Scenario:Verify GCSMove plugin error message for invalid bucket name in Destination Path
    Given Open Datafusion Project to configurse pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "GCS Move" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "GCS Move"
    Then Enter GCSMove property source path "gcsCsvFile"
    Then Enter GCSMove property destination path "gcsInvalidBucketName"
    Then Verify GCS Move property "destPath" invalid bucket name error message is displayed for bucket "gcsInvalidBucketName"
