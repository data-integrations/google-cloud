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
    Then Enter input plugin property: "path" with value: "gcsInvalidBucketName"
    Then Select GCS property format "csv"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "path" is displaying an in-line error message: "errorMessageInvalidBucketName"

  @BQ_SOURCE_DATATYPE_TEST @GCS_SINK_TEST @GCS_Sink_Required
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

  @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario: To verify and validate the Error message in pipeline logs after deploy with invalid bucket path
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS property "path" as macro argument "gcsSinkPath"
    Then Select GCS property format "csv"
    Then Click on the Validate button
    Then Close the GCS properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "gcsInvalidBucketNameSink" for key "gcsSinkPath"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state and check if any error occurs
    Then Open and capture pipeline preview logs
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "gcsInvalidBucketNameSink" for key "gcsSinkPath"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                           |
      | ERROR | errorMessageInvalidBucketNameSink |
    Then Close the pipeline logs
