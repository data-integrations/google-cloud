Feature: Verify Errors

  @TC-Error-1
  Scenario: Verify Error validation in reference field
  Given Open Datafusion Project to configure pipeline
  When Source is GCS bucket
  When Target is BigQuery
  Then Link Source and Sink to establish connection
  Then Enter the GCS Properties with GCS bucket "gcsCsvbucket" and format "gcsCSVFileFormat" by entering blank referenceName
  Then Verify reference name is mandatory

  @TC-Error-2
  Scenario: Verify Error validation in path field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with format "gcsCSVFileFormat" by entering blank path
    Then Verify path is mandatory

  @TC-plugin-857 @GCS
  Scenario: Verify automatic datatype detection of Timestamp in GCS
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "timestamp_test" and format "gcsCSVFileFormat"
    Then verify the datatype

  @TC-plugin-855 @GCS
  Scenario: Verify File encoding types
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsCsvbucket" , format "gcsCSVFileFormat" and fileEncoding 32
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Open Logs

  @TC-Wrong_OutputFieldpath_test @GCS
  Scenario: To verify Error message in wrong output field path value
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "output_fieldtest", format "gcsCSVFileFormat" and Pathfield "wrongpath" value
    Then Verify OutputPath field Error Message

  @TC-Wrong-Regex-Path @GCS
  Scenario: To verify Wrong-Regex-Path
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsCsvbucket" and format "gcsCSVFileFormat" using Regexpath filter
    Then Verify the schema in output
    Then Validate GCS properties
    Then Capture output schema
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "failed"
    Then Click on PreviewData for GCS
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview