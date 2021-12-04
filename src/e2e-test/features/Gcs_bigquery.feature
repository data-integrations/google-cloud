Feature: Design-time

  @TC-Design_time_mandatoryfields @GCS
  Scenario: To verify data is getting transferred from GCS to BigQuery with Mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsCsvbucket" and format "gcsCSVFileFormat"
    Then Validate GCS properties
    Then Capture output schema
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for GCS
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed
    Then Get Count of no of records transferred to BigQuery in "gcsBqTableName"
    Then Delete the table "tableDemo"


  @TC-Designtime_OutputFieldpath_test @GCS
  Scenario: To verify outputField path from GCS to BigQuery
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "output_fieldtest", format "gcsCSVFileFormat" and Pathfield "gcspathField" value
    Then Validate GCS properties
    Then Capture output schema
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for GCS
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview

  @TC-OverrideDatatype @GCS
  Scenario: To verify OverrideDatatype field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "output_fieldtest" and format "gcsCSVFileFormat" using OverrideDatatype
    Then Verify the schema in output
    Then Validate GCS properties
    Then Capture output schema
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for GCS
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview

  @TC-DelimiterField @GCS
  Scenario: To verify Delimiter field in delimiter format
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsDelimitedbucket" and format "gcsTextFileFormat" with Delimiter field
    Then Verify the schema in output
    Then Validate GCS properties
    Then Capture output schema
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for GCS
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview

  @TC-MaxMinSplitSize @GCS
  Scenario: To verify Succcessful data transfer from GCS to BigQuery with blob file by entering MaxMinSplitSize
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsBlobbucket" and format "gcsblobFileFormat" with MaxMinField
    Then Verify the schema in output
    Then Validate GCS properties
    Then Capture output schema
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for GCS
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview

  @TC-Regex-Path @GCS
  Scenario: To verify Succcessful data transfer from GCS to BigQuery using Regex path filter
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
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for GCS
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview