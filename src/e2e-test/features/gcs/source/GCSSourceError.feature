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
      | path            |
      | format          |

  Scenario: To verify Error message for invalid bucket name
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS property path "gcsInvalidBucketName"
    Then Select GCS property format "csv"
    Then Verify invalid bucket name error message is displayed for bucket "gcsInvalidBucketName"

  @GCS_OUTPUT_FIELD_TEST
  Scenario: To verify Error message for incorrect output path field value
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsOutputFieldTestFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Enter GCS source property path field "gcsInvalidPathField"
    Then Verify Output Path field Error Message for incorrect path field "gcsInvalidPathField"

  @GCS_OUTPUT_FIELD_TEST
  Scenario: To verify Error for incorrect override field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsOutputFieldTestFile"
    Then Select GCS property format "csv"
    Then Enter GCS source property override field "gcsInvalidOverrideField" and data type "gcsOverrideFloatDataType"
    Then Toggle GCS source property skip header to true
    Then Verify get schema fails with error

  @GCS_CSV_TEST @BQ_SINK_TEST
  Scenario: To verify schema detection fails for incorrect Regex-Path as input path is not found
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Enter GCS source property regex path filter "gcsIncorrectRegexPath"
    Then Toggle GCS source property skip header to true
    Then Click on the Get Schema button
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidPath" on the header

  @GCS_CSV_TEST
  Scenario: To verify Error message for invalid minimum split size
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Enter GCS source property minimum split size "gcsMinSplitSize" and maximum split size "gcsInvalidMinSplitSize"
    Then Click on the Get Schema button
    Then Verify that the Plugin Property: "minSplitSize" is displaying an in-line error message: "errorMessageIncorrectMinimumSplitSize"

  @GCS_CSV_TEST
  Scenario: To verify Error message for invalid maximum split size
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Enter GCS source property minimum split size "gcsInvalidMaxSplitSize" and maximum split size "gcsMaxSplitSize"
    Then Click on the Get Schema button
    Then Verify that the Plugin Property: "maxSplitSize" is displaying an in-line error message: "errorMessageIncorrectMaximumSplitSize"

  @GCS_CSV_TEST
  Scenario: To verify Error message for invalid regex path filter
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Enter input plugin property: "fileRegex" with value: "gcsInvalidRegexPathFilter"
    Then Click on the Get Schema button
    Then Verify that the Plugin Property: "fileRegex" is displaying an in-line error message: "errorMessageIncorrectRegexPathFilter"

  @GCS_CSV_TEST
  Scenario: To verify Error message for invalid file system properties
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Enter textarea plugin property: "fileSystemProperties" with value: "gcsInvalidFileSysProperty"
    Then Click on the Get Schema button
    Then Verify that the Plugin Property: "fileSystemProperties" is displaying an in-line error message: "errorMessageIncorrectFileSystemProperties"

  @GCS_CSV_TEST
  Scenario: To verify Error message for invalid reference name
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Enter input plugin property: "referenceName" with value: "gcsInvalidRefName"
    Then Click on the Get Schema button
    Then Verify that the Plugin Property: "referenceName" is displaying an in-line error message: "errorMessageInvalidReferenceName"
