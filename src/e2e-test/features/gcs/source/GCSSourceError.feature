@GCS_Source
Feature: GCS source - Verify GCS Source plugin error scenarios

  Scenario Outline:Verify GCS Source properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter the GCS properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | path            |
      | format          |

  Scenario: To verify Error message for invalid bucket name
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS property path "gcsInvalidBucketName"
    Then Select GCS property format "csv"
    Then Verify invalid bucket name error message is displayed for bucket "gcsInvalidBucketName"

  @GCS_OUTPUT_FIELD_TEST
  Scenario: To verify Error message for incorrect output path field value
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsOutputFieldTestFile"
    Then Select GCS property format "csv"
    Then Enter GCS source property path field "gcsInvalidPathField"
    Then Verify Output Path field Error Message for incorrect path field "gcsInvalidPathField"

  @GCS_DELIMITED_TEST
  Scenario: To verify Error for incorrect delimiter
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsDelimitedFile"
    Then Select GCS property format "delimited"
    Then Enter GCS property delimiter "gcsIncorrectDelimiter"
    Then Toggle GCS source property skip header to true
    Then Verify get schema fails with error

  @GCS_OUTPUT_FIELD_TEST
  Scenario: To verify Error for incorrect override field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsOutputFieldTestFile"
    Then Select GCS property format "csv"
    Then Enter GCS source property override field "gcsInvalidOverrideField" and data type "gcsOverrideDataType"
    Then Toggle GCS source property skip header to true
    Then Verify get schema fails with error

  @GCS_CSV_TEST @BQ_SINK_TEST
  Scenario: To verify Pipeline preview gets failed for incorrect Regex-Path
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Enter GCS source property regex path filter "gcsIncorrectRegexPath"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsCsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "failed"
