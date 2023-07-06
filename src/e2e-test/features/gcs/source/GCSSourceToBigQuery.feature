@GCS_Source
Feature: GCS source - Verification of GCS to BQ successful data transfer

  @GCS_CSV_TEST @BQ_SINK_TEST
  Scenario: To verify data is getting transferred from GCS to BigQuery with Mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsCsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for BigQuery sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table

  @GCS_OUTPUT_FIELD_TEST @BQ_SINK_TEST
  Scenario: To verify successful data transfer from GCS to BigQuery with outputField
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsOutputFieldTestFile"
    Then Select GCS property format "csv"
    Then Enter GCS source property path field "gcsPathField"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsOutputFieldTestFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Verify output field "gcsPathField" in target BigQuery table contains path of the source GcsBucket "gcsOutputFieldTestFile"

  @GCS_OUTPUT_FIELD_TEST @BQ_SINK_TEST @GCS_Source_Required
  Scenario: To verify Successful GCS to BigQuery data transfer with Datatype override
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsOutputFieldTestFile"
    Then Select GCS property format "csv"
    Then Enter GCS source property override field "gcsOverrideField" and data type "gcsOverrideFloatDataType"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsOverrideSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Verify datatype of field "gcsOverrideField" is overridden to data type "gcsOverrideFloatDataType" in target BigQuery table

  @GCS_DELIMITED_TEST @BQ_SINK_TEST
  Scenario: To verify Successful GCS to BigQuery data transfer with Delimiter
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsDelimitedFile"
    Then Select GCS property format "delimited"
    Then Enter GCS property delimiter "gcsDelimiter"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsDelimitedFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table

  @GCS_BLOB_TEST @BQ_SINK_TEST
  Scenario: To verify Successful GCS to BigQuery data transfer with blob file by entering MaxMinSplitSize
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsBlobFile"
    Then Select GCS property format "blob"
    Then Enter GCS source property minimum split size "gcsMinSplitSize" and maximum split size "gcsMaxSplitSize"
    Then Validate output schema with expectedSchema "gcsBlobFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table

  @GCS_CSV_TEST @BQ_SINK_TEST
  Scenario: To verify Successful GCS to BigQuery data transfer using Regex path filter
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Enter GCS source property regex path filter "gcsRegexPathFilter"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsCsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table

  @GCS_CSV_TEST @BQ_SINK_TEST
  Scenario: To verify Successful GCS to BigQuery data transfer with sample size field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Enter input plugin property: "sampleSize" with value: "gcsSampleSize"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsCsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Validate the values of records transferred from GCS bucket file is equal to the values of target BigQuery table

  @GCS_CSV_TEST @BQ_SINK_TEST
  Scenario: To verify Successful GCS to BigQuery data transfer with with enable quoted values true
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Click plugin property: "switch-enableQuotedValues"
    Then Validate output schema with expectedSchema "gcsCsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Validate the values of records transferred from GCS bucket file is equal to the values of target BigQuery table

  @GCS_CSV_TEST @BQ_SINK_TEST
  Scenario: To verify Successful GCS to BigQuery data transfer with allow empty input true
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Select radio button plugin property: "ignoreNonExistingFolders" with value: "true"
    Then Validate output schema with expectedSchema "gcsCsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Validate the values of records transferred from GCS bucket file is equal to the values of target BigQuery table
