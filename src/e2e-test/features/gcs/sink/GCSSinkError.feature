@GCS_Sink
Feature: GCS sink - Verify GCS Sink plugin error scenarios

  Scenario Outline:Verify GCS Sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Sink is GCS
    Then Open GCS sink properties
    Then Enter the GCS properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | path            |
      | format          |

  @GCS_SINK_TEST
  Scenario:Verify GCS Sink properties validation errors for invalid reference name
    Given Open Datafusion Project to configure pipeline
    When Sink is GCS
    Then Open GCS sink properties
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "gcsInvalidRefName"
    Then Enter GCS sink property path
    Then Select GCS property format "csv"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "referenceName" is displaying an in-line error message: "errorMessageInvalidReferenceName"

  @GCS_SINK_TEST
  Scenario:Verify GCS Sink properties validation errors for invalid file system properties
    Given Open Datafusion Project to configure pipeline
    When Sink is GCS
    Then Open GCS sink properties
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter textarea plugin property: "fileSystemProperties" with value: "gcsInvalidFileSysProperty"
    Then Enter input plugin property: "referenceName" with value: "gcsReferenceName"
    Then Enter GCS sink property path
    Then Select GCS property format "csv"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "fileSystemProperties" is displaying an in-line error message: "errorMessageIncorrectFileSystemProperties"

  Scenario:Verify GCS Sink properties validation errors for invalid bucket path
    Given Open Datafusion Project to configure pipeline
    When Sink is GCS
    Then Open GCS sink properties
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "gcsReferenceName"
    Then Enter GCS source property path "gcsInvalidBucketName"
    Then Select GCS property format "csv"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "path" is displaying an in-line error message: "errorMessageInvalidBucketName"

  @BQ_SOURCE_DATATYPE_TEST @GCS_SINK_TEST
  Scenario: To verify error message when unsupported format is used in GCS sink with multiple datatypes provided in source table
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "bqSourceSchemaDatatype"
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS sink property path
    Then Select GCS property format "csv"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "format" is displaying an in-line error message: "errorMessageInvalidFormat"
